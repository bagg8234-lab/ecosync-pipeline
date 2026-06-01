import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd
from data_generator import generate_generation_data


def validate_generation_stats(data: list[dict]) -> tuple[bool, list, object]:
    """
    Great Expectations로 발전량 데이터 통계 검증
    """
    df = pd.DataFrame(data)

    context = gx.get_context(mode="ephemeral")

    datasource = context.sources.add_pandas("pandas_source")
    data_asset = datasource.add_dataframe_asset("generation_asset")
    batch_request = data_asset.build_batch_request(dataframe=df)

    # Expectation Suite 생성
    context.add_or_update_expectation_suite("generation_suite")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="generation_suite"
    )

    # 규칙 정의
    validator.expect_column_values_to_be_between(
        column="generation_kwh",
        min_value=0,
        max_value=10000
    )
    validator.expect_column_values_to_not_be_null(column="generation_kwh")
    validator.expect_column_values_to_not_be_null(column="city")

    # 검증 실행
    results = validator.validate()
    success = results.success
    failures = [
        str(r) for r in results.results if not r.success
    ]
    return success, failures, results


def save_html_report(results, report_path: str = "/app/logs/ge_report.html"):
    """검증 결과를 HTML 리포트로 저장"""
    report_data = []
    for r in results.results:
        report_data.append({
            "expectation_type": r.expectation_config.expectation_type,
            "column": r.expectation_config.kwargs.get("column", ""),
            "success": r.success,
            "unexpected_count": r.result.get("unexpected_count", 0),
            "unexpected_percent": r.result.get("unexpected_percent", 0),
            "partial_unexpected_list": r.result.get("partial_unexpected_list", [])
        })

    html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <title>EcoSync 데이터 품질 리포트</title>
        <style>
            body {{ font-family: Arial; margin: 40px; background: #f5f5f5; }}
            h1 {{ color: #333; }}
            .success {{ color: green; font-weight: bold; }}
            .fail {{ color: red; font-weight: bold; }}
            table {{ border-collapse: collapse; width: 100%; background: white; }}
            th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
            th {{ background: #4CAF50; color: white; }}
            tr:nth-child(even) {{ background: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h1>EcoSync 데이터 품질 리포트</h1>
        <p>전체 결과: <span class="{'success' if results.success else 'fail'}">
            {'✅ 통과' if results.success else '❌ 실패'}
        </span></p>
        <table>
            <tr>
                <th>검증 항목</th>
                <th>컬럼</th>
                <th>결과</th>
                <th>이상치 수</th>
                <th>이상치 비율</th>
                <th>이상치 값</th>
            </tr>
    """

    for r in report_data:
        status = "✅" if r["success"] else "❌"
        html += f"""
            <tr>
                <td>{r["expectation_type"]}</td>
                <td>{r["column"]}</td>
                <td>{status}</td>
                <td>{r["unexpected_count"]}</td>
                <td>{r["unexpected_percent"]}%</td>
                <td>{r["partial_unexpected_list"]}</td>
            </tr>
        """

    html += """
        </table>
    </body>
    </html>
    """

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML 리포트 저장: {report_path}")


if __name__ == "__main__":
    print("Great Expectations 검증 테스트")

    # 정상 데이터
    normal_data = generate_generation_data(10)
    success, failures, results = validate_generation_stats(normal_data)
    print(f"정상 데이터: {'통과' if success else '실패'}")

    # 오염된 데이터
    corrupted_data = generate_generation_data(10)
    corrupted_data[0]['generation_kwh'] = -100.0
    corrupted_data[1]['city'] = None

    success, failures, results = validate_generation_stats(corrupted_data)
    print(f"오염된 데이터: {'통과' if success else '실패'}")
    if failures:
        print("실패 항목:")
        for f in failures:
            print(f)

    # HTML 리포트 저장
    save_html_report(results)