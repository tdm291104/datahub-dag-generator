# Demo Script — DataHub DAG Generator

## Mở đầu (30 giây)

> *"Data pipeline thường có 5-10 bảng phụ thuộc nhau. Ai đảm bảo DAG chạy đúng thứ tự? Ai nhớ bảng nào cần check freshness, bảng nào có PII? Thông thường đây là việc data engineer phải làm thủ công — đọc docs, nhớ convention, viết tay từng task."*
>
> *"DataHub DAG Generator giải quyết bài toán này: agent đọc trực tiếp lineage và metadata trong DataHub, tự suy ra các quality check cần thiết, rồi sinh ra Airflow DAG hoàn chỉnh — không cần tay người."*

---

## Phần 1 — DataHub có gì (1 phút)

Mở **http://localhost:9002**, tìm `mart_daily_summary`, mở tab **Lineage**:

- Show graph: `raw_trips → staging_trips → mart_daily_summary`

Click vào `staging_trips`, show panel bên phải:
- Tags: `daily_refresh`, `pii`, `time_series`
- Glossary: `Freshness SLA`

> *"Bảng này có tag `daily_refresh` và glossary `Freshness SLA` — nghĩa là phải check xem data có được cập nhật đúng hạn không. Nó cũng có tag `pii` — nghĩa là cần audit log mỗi lần access."*

Click vào `mart_daily_summary`:
- Tags: `daily_refresh`
- Glossary: `Freshness SLA`, `Empty Load`

> *"Bảng mart có thêm glossary `Empty Load` — nghĩa là phải kiểm tra bảng không được rỗng sau khi chạy ETL."*

> *"Agent sẽ đọc chính xác những metadata này để quyết định task nào cần thêm vào DAG."*

---

## Phần 2 — Generate DAG (1 phút)

```bash
datahub-dag --target mart_daily_summary --instance nyc_taxi --dry-run --mode script
```

Chỉ vào output và kết nối trực tiếp với metadata vừa thấy:

> *"`freshness_check_staging_trips` — xuất hiện vì tag `daily_refresh` và glossary `Freshness SLA`."*
>
> *"`data_audit_staging_trips` — xuất hiện vì tag `pii`."*
>
> *"`validate_row_count_mart_daily_summary` — xuất hiện vì glossary `Empty Load`."*
>
> *"Thứ tự task: `raw_trips → staging_trips → mart_daily_summary` — khớp đúng lineage graph."*
>
> *"Và quan trọng: LLM không được phép viết shell command. Toàn bộ logic freshness check và audit do ứng dụng kiểm soát — LLM chỉ đọc metadata và quyết định check nào cần thêm."*

---

## Phần 3 — Freshness check hoạt động thực tế (1.5 phút)

> *"DAG không chỉ generate code template — freshness check thực sự so sánh timestamps trong data."*

> *"Chúng tôi có 2 bộ data: một bộ bình thường và một bộ mô phỏng pipeline bị lỗi — `staging_trips` đứng 9 ngày so với `raw_trips`."*

Mở **http://localhost:8081**, show 2 DAG song song:

| DAG | Database | Kỳ vọng |
|---|---|---|
| `nyc_taxi_pipeline` | `nyc_taxi.db` (bình thường) | Tất cả pass ✅ |
| `nyc_taxi_pipeline_stale` | `nyc_taxi_pipeline.db` (stale) | `freshness_check_staging_trips` fail ❌ |

Trigger cả 2 DAG, đợi ~30-40 giây:

- `nyc_taxi_pipeline`: tất cả task **xanh** ✅
- `nyc_taxi_pipeline_stale`: `freshness_check_staging_trips` **đỏ** ❌, toàn bộ downstream bị block

> *"`freshness_check_staging_trips` phát hiện staging lag 9 ngày so với raw — pipeline dừng lại đúng chỗ, không cho phép data stale chạy tiếp xuống mart."*

---

## Phần 4 — Tạo GitHub PR (1 phút)

```bash
datahub-dag --target mart_daily_summary --instance nyc_taxi --mode script --pr
```

Mở PR vừa tạo trên GitHub, show phần **PR description**:

> *"PR description tự động liệt kê từng stage: upstream nào, metadata signal nào kích hoạt task gì. Reviewer hiểu toàn bộ lý do mà không cần đọc DAG source."*
>
> *"Từ lệnh một dòng đến PR sẵn sàng review — không cần viết tay một dòng Airflow code nào."*

---

## Kết (15 giây)

> *"DataHub DAG Generator không chỉ generate code — nó đọc lineage thật, đọc metadata thật, phát hiện data health issue thật. Mỗi lần pipeline thay đổi, một lệnh là DAG tự cập nhật theo."*

---

## Backup — Câu hỏi về security

> *"LLM bị sandbox hoàn toàn — nó chỉ được phép gọi hai custom tool: `render_airflow_dag` và `datahub_write_back`. Toàn bộ nội dung `bash_command` trong BashOperator do ứng dụng sinh ra, không phải LLM. LLM không thể inject lệnh tùy ý."*

---

## Checklist trước khi demo

```bash
# 1. DataHub đang chạy
curl -s http://localhost:8080/config | grep version

# 2. Airflow đang chạy
curl -s http://localhost:8081/api/v2/monitor/health

# 3. Cả 2 DAG đã có trong Airflow
docker exec datahub-dag-airflow-airflow-1 airflow dags list | grep nyc

# 4. Test dry-run nhanh
datahub-dag --target mart_daily_summary --instance nyc_taxi --dry-run --mode script
```
