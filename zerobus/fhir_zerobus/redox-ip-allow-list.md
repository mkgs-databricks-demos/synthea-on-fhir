# Redox Staging IP Allow List

If demoing or using ZeroBus with Redox, the workspace may need to have an allow list enabled for external IPs to reach the Databricks Apps endpoints.   This may also be accomplished with an API gateway where the API gateway allows public internet traffic, but the Databricks workspace only requires opening up the IP range from the gateway with authenticated users only.  

## IP Ranges

The `redox-staging-zerobus` allow list includes the following Redox staging IP ranges:

- `52.205.60.0/24`
- `52.205.89.0/24`
- `52.205.169.0/24`

## Check if the IP Access List Feature is Enabled

```bash
databricks workspace-conf get-status enableIpAccessLists
```

## View All Existing IP Access Lists

```bash
databricks ip-access-lists list
```

To get details for a specific list by ID:

```bash
databricks ip-access-lists get <IP_ACCESS_LIST_ID>
```

## Create the Allow List

If the `redox-staging-zerobus` list does not yet exist, create it:

```bash
databricks ip-access-lists create --json '{
  "label": "redox-staging-zerobus",
  "list_type": "ALLOW",
  "ip_addresses": [
    "52.205.60.0/24",
    "52.205.89.0/24",
    "52.205.169.0/24"
  ]
}'
```

## Update the Allow List

If the list already exists and needs to be updated, first get the list ID from `databricks ip-access-lists list`, then replace its contents:

```bash
databricks ip-access-lists replace <IP_ACCESS_LIST_ID> --json '{
  "label": "redox-staging-zerobus",
  "list_type": "ALLOW",
  "ip_addresses": [
    "52.205.60.0/24",
    "52.205.89.0/24",
    "52.205.169.0/24"
  ]
}'
```
