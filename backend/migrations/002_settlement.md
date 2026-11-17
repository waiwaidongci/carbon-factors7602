# 002 结算包

新增 `settlement_packages` 表（SQLAlchemy 启动自动建表，存量库无感升级）：

|列|含义|
|---|---|
|run_id|对应已复核报告，唯一|
|status|queued/running/completed/failed|
|epoch|栅栏令牌：每次申请+1；worker 每次落盘与发布都带 epoch 条件更新，旧执行者晚到写不进|
|worker / locked_until|执行者租约；过期未心跳即可被其他执行者按原 epoch 接管续跑|
|rows_done / chunks_done / kg_sum|断点：已原子落盘的行数、分块数与 Decimal 累计排放|
|total_rows / total_kg|快照总行数；发布后排放合计（与报告一致的 Decimal 文本）|
|file_path / checksum / size_bytes|下载入口，仅在文件完整组装并原子 rename 后由条件 UPDATE 切换；重新申请期间旧文件仍可下载|

产物文件在共享卷 `ARTIFACTS_DIR`（默认 `/app/artifacts`，compose 卷 `report-files`）：
`settlements/work-{id}-e{epoch}/chunk-*.csv` 为分块落盘（tmp+rename 原子提交），全部完成后按序组装为 `settlements/pkg-{id}-e{epoch}.csv` 并原子切换入口；旧 epoch 产物随即清理。

生成读 `carbon_runs.snapshot` 冻结快照（底稿后改不影响），服务端游标按 `部门→活动→冻结序号`（COLLATE "C"）全局排序流式输出，进程内存与分块大小有界（远低于 64MiB），分块大小变化不改变文件字节。
