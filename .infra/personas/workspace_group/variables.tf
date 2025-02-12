variable "workspace_id" {
  type      = set(string)
  sensitive = false
  nullable  = false
}

variable "display_name" {
  type      = string
  sensitive = false
  nullable  = false
}

variable "permissions" {
  type      = set(string)
  sensitive = false
  default   = ["USER"]
}

variable "members" {
  type      = map(string)
  sensitive = false
  default   = {}
}