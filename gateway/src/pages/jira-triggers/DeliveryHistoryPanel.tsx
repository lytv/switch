import RefreshOutlined from "@mui/icons-material/RefreshOutlined";
import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  IconButton,
  Stack,
  Tooltip,
  Typography,
} from "@mui/material";
import type { GridColDef } from "@mui/x-data-grid";
import { useMemo } from "react";
import DataTable from "../../components/DataTable";
import type { JiraDeliveryDetail } from "../../data/api";
import { useJiraDeliveries } from "../../data/hooks";
import { MONO_SX } from "../../theme/hootFormat";

function statusColor(
  status: string,
): "default" | "success" | "error" | "warning" | "info" {
  switch (status) {
    case "delivered":
      return "success";
    case "error":
      return "error";
    case "pending":
      return "info";
    case "suppressed_burst":
    case "suppressed_cooldown":
    case "suppressed_dedupe":
      return "warning";
    default:
      return "default";
  }
}

function roomSummary(row: JiraDeliveryDetail): string {
  if (!row.room_results.length) {
    return row.error ?? "—";
  }
  return row.room_results
    .map((r) => {
      const name = r.room_name ?? r.room_id;
      if (r.status === "ok") return `${name}: ok`;
      return `${name}: ${r.status}${r.error ? ` (${r.error})` : ""}`;
    })
    .join("; ");
}

export default function DeliveryHistoryPanel() {
  const { data, loading, error, refetch } = useJiraDeliveries(50);
  const rows = useMemo(() => data?.deliveries ?? [], [data]);

  const columns = useMemo<GridColDef<JiraDeliveryDetail>[]>(
    () => [
      {
        field: "created_at",
        headerName: "When",
        width: 180,
        valueGetter: (_value, row) => String(row.created_at).replace("T", " ").slice(0, 19),
      },
      {
        field: "issue_key",
        headerName: "Issue",
        width: 110,
        renderCell: ({ row }) => (
          <Typography variant="body2" sx={MONO_SX}>
            {row.issue_key}
          </Typography>
        ),
      },
      {
        field: "rule_name",
        headerName: "Rule",
        flex: 1,
        minWidth: 140,
        valueGetter: (_value, row) => row.rule_name || row.rule_id,
      },
      {
        field: "status",
        headerName: "Status",
        width: 160,
        renderCell: ({ row }) => (
          <Chip size="small" label={row.status} color={statusColor(row.status)} />
        ),
      },
      {
        field: "matched",
        headerName: "Matched rules",
        width: 120,
        sortable: false,
        valueGetter: (_value, row) =>
          row.matched_rule_ids.length
            ? `${row.matched_rule_ids.length}`
            : "—",
      },
      {
        field: "rooms",
        headerName: "Room results",
        flex: 2,
        minWidth: 220,
        sortable: false,
        valueGetter: (_value, row) => roomSummary(row),
      },
    ],
    [],
  );

  return (
    <Box sx={{ mt: 3 }}>
      <Stack direction="row" alignItems="center" mb={1.5} spacing={1}>
        <Typography variant="h6">Delivery history</Typography>
        <Tooltip title="Refresh">
          <IconButton
            size="small"
            aria-label="Refresh delivery history"
            onClick={() => refetch()}
          >
            <RefreshOutlined fontSize="small" />
          </IconButton>
        </Tooltip>
      </Stack>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
        Recent rule matches and per-room send outcomes. Retained for{" "}
        {data
          ? `${Math.round(data.retain_seconds / 3600)}h / ${data.max_rows} rows`
          : "a bounded window"}
        .
      </Typography>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      {loading && !data ? (
        <CircularProgress size={24} />
      ) : rows.length === 0 ? (
        <Alert severity="info">
          No deliveries yet. When a webhook matches a rule, the outcome appears
          here — including burst, cool-down, and dedupe suppressions.
        </Alert>
      ) : (
        <DataTable rows={rows} columns={columns} />
      )}
    </Box>
  );
}
