"""
pipeline.process_ge_batch 단위 테스트
- DB 저장(upsert_generation/upsert_demand), MinIO 저장(upload_raw_data) 실패 시
  send_to_dlq가 올바른 error_type으로 호출되는지 검증
- DB 저장 실패는 psycopg2.OperationalError(실제 연결 장애)만 system_error이고,
  그 외 예외(예: NotNullViolation)는 data_error로 분류됨
- MinIO 저장 실패도 동일하게 botocore ConnectionError 계열(실제 연결 장애)만 system_error이고,
  그 외 예외(예: ClientError)는 data_error로 분류됨
- GE 검증 실행 자체가 실패(환경/리소스 문제 등)한 경우도 system_error로 분류되어
  배치 내 모든 레코드가 DLQ로 전송됨
"""
import psycopg2
from botocore.exceptions import ClientError, EndpointConnectionError
from unittest.mock import patch, MagicMock

# dead_letter_queue 모듈은 import 시점에 실제 Kafka 브로커 연결을 시도하므로,
# pipeline import 전에 KafkaProducer를 목으로 대체해 실제 인프라 접속을 막는다.
with patch("kafka.KafkaProducer", return_value=MagicMock()):
    import pipeline


def _ge_pass():
    """GE 통계 검증을 모두 통과했다고 가정한 (success, failures, results) 반환값"""
    return True, [], None


class TestProcessGeBatch:
    def setup_method(self):
        self.minio_client = MagicMock()
        self.target_buffer = []

    @patch("pipeline.send_to_dlq")
    @patch("pipeline.upload_raw_data")
    @patch("pipeline.upsert_generation")
    @patch("pipeline.validate_generation_stats")
    def test_generation_db_save_failure_sends_to_dlq_with_system_error(
        self, mock_validate_stats, mock_upsert_generation, mock_upload_raw_data, mock_send_to_dlq
    ):
        """발전량 DB 연결 장애(psycopg2.OperationalError) 시 system_error로 DLQ 전송"""
        mock_validate_stats.return_value = _ge_pass()
        mock_upsert_generation.side_effect = psycopg2.OperationalError("DB 연결 끊김")
        record = {"id": "gen-1", "city": "서울", "generation_kwh": 120.5}

        pipeline.process_ge_batch([record], "generation", self.minio_client, self.target_buffer)

        mock_send_to_dlq.assert_called_with(
            record, "DB 연결 실패: DB 연결 끊김", "generation", error_type="system_error"
        )
        mock_upload_raw_data.assert_not_called()
        assert self.target_buffer == []

    @patch("pipeline.send_to_dlq")
    @patch("pipeline.upload_raw_data")
    @patch("pipeline.upsert_generation")
    @patch("pipeline.validate_generation_stats")
    def test_generation_minio_connection_error_sends_to_dlq_with_system_error(
        self, mock_validate_stats, mock_upsert_generation, mock_upload_raw_data, mock_send_to_dlq
    ):
        """MinIO 연결 장애(botocore ConnectionError) 시 system_error로 DLQ 전송"""
        mock_validate_stats.return_value = _ge_pass()
        mock_upload_raw_data.side_effect = EndpointConnectionError(endpoint_url="http://minio:9000")
        record = {"id": "gen-2", "city": "부산", "generation_kwh": 88.0}

        pipeline.process_ge_batch([record], "generation", self.minio_client, self.target_buffer)

        mock_upsert_generation.assert_called_once_with(record)
        mock_send_to_dlq.assert_called_with(
            record,
            'MinIO 연결 실패: Could not connect to the endpoint URL: "http://minio:9000"',
            "generation",
            error_type="system_error",
        )
        assert self.target_buffer == []

    @patch("pipeline.send_to_dlq")
    @patch("pipeline.upload_raw_data")
    @patch("pipeline.upsert_generation")
    @patch("pipeline.validate_generation_stats")
    def test_generation_minio_client_error_sends_to_dlq_with_data_error(
        self, mock_validate_stats, mock_upsert_generation, mock_upload_raw_data, mock_send_to_dlq
    ):
        """MinIO 저장 실패가 연결 장애가 아닌 경우(예: ClientError)는 data_error로 분류"""
        mock_validate_stats.return_value = _ge_pass()
        mock_upload_raw_data.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "bucket missing"}}, "PutObject"
        )
        record = {"id": "gen-3", "city": "대전", "generation_kwh": 45.0}

        pipeline.process_ge_batch([record], "generation", self.minio_client, self.target_buffer)

        mock_upsert_generation.assert_called_once_with(record)
        mock_send_to_dlq.assert_called_with(
            record,
            "MinIO 저장 실패: An error occurred (NoSuchBucket) when calling the PutObject operation: bucket missing",
            "generation",
            error_type="data_error",
        )
        assert self.target_buffer == []

    @patch("pipeline.send_to_dlq")
    @patch("pipeline.upload_raw_data")
    @patch("pipeline.upsert_demand")
    @patch("pipeline.validate_demand_stats")
    def test_demand_db_save_failure_sends_to_dlq_with_system_error(
        self, mock_validate_stats, mock_upsert_demand, mock_upload_raw_data, mock_send_to_dlq
    ):
        """소비량 DB 연결 장애(psycopg2.OperationalError) 시 system_error로 DLQ 전송"""
        mock_validate_stats.return_value = _ge_pass()
        mock_upsert_demand.side_effect = psycopg2.OperationalError("DB 연결 끊김")
        record = {"id": "dem-1", "city": "대구", "demand_kwh": 50.0}

        pipeline.process_ge_batch([record], "demand", self.minio_client, self.target_buffer)

        mock_send_to_dlq.assert_called_with(
            record, "DB 연결 실패: DB 연결 끊김", "demand", error_type="system_error"
        )
        mock_upload_raw_data.assert_not_called()
        assert self.target_buffer == []

    @patch("pipeline.send_to_dlq")
    @patch("pipeline.upload_raw_data")
    @patch("pipeline.upsert_generation")
    @patch("pipeline.validate_generation_stats")
    def test_generation_db_integrity_error_sends_to_dlq_with_data_error(
        self, mock_validate_stats, mock_upsert_generation, mock_upload_raw_data, mock_send_to_dlq
    ):
        """DB 저장 실패가 연결 장애가 아닌 경우(예: id 누락으로 인한 NotNullViolation)는 data_error로 분류
        (id는 PRIMARY KEY이자 ON CONFLICT 대상이라 UniqueViolation은 발생하지 않음)"""
        mock_validate_stats.return_value = _ge_pass()
        mock_upsert_generation.side_effect = psycopg2.errors.NotNullViolation(
            'null value in column "id" violates not-null constraint'
        )
        record = {"id": None, "city": "인천", "generation_kwh": 60.0}

        pipeline.process_ge_batch([record], "generation", self.minio_client, self.target_buffer)

        mock_send_to_dlq.assert_called_with(
            record,
            'DB 저장 실패: null value in column "id" violates not-null constraint',
            "generation",
            error_type="data_error",
        )
        mock_upload_raw_data.assert_not_called()
        assert self.target_buffer == []

    @patch("pipeline.send_to_dlq")
    @patch("pipeline.upload_raw_data")
    @patch("pipeline.upsert_generation")
    @patch("pipeline.validate_generation_stats")
    def test_generation_ge_execution_failure_sends_all_records_to_dlq_with_system_error(
        self, mock_validate_stats, mock_upsert_generation, mock_upload_raw_data, mock_send_to_dlq
    ):
        """GE 검증 실행 자체가 실패하면 배치 내 모든 레코드가 system_error로 DLQ 전송되고 DB/MinIO 저장은 스킵됨"""
        mock_validate_stats.side_effect = Exception("GE 컨텍스트 생성 실패")
        record1 = {"id": "gen-5", "city": "서울", "generation_kwh": 30.0}
        record2 = {"id": "gen-6", "city": "부산", "generation_kwh": 40.0}

        pipeline.process_ge_batch([record1, record2], "generation", self.minio_client, self.target_buffer)

        mock_send_to_dlq.assert_called_with(
            record2, "GE 검증 실행 실패: GE 컨텍스트 생성 실패", "generation", error_type="system_error"
        )
        assert mock_send_to_dlq.call_count == 2
        mock_upsert_generation.assert_not_called()
        mock_upload_raw_data.assert_not_called()
        assert self.target_buffer == []
