resource "databricks_group" "group" {
  display_name = var.display_name
}

resource "databricks_mws_permission_assignment" "workspace_assignment" {
  for_each = var.workspace_id

  permissions  = var.permissions
  workspace_id = each.value
  principal_id = databricks_group.group.id
}

resource "databricks_group_member" "members" {
  for_each = var.members

  group_id  = databricks_group.group.id
  member_id = each.value
}