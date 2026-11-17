// API实体字段约定：来源{id,natural_key,data}、口径{id,version,data}、报告{id,status,snapshot,checksum,result,approved}、计划{next_at,enabled}、结算包{run_id,status,epoch,rows_done,total_rows,total_kg,file_path,checksum}。
export const ENTITY_KEYS=['sources','policies','runs','schedules','deliveries','settlements'];
