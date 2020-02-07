create_task_subset <- function(nwis_uv_pull_plan) {
  tasks <- names(nwis_uv_pull_plan)[c(-1)]
  return(tasks)
}