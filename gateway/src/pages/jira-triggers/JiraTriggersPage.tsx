import AddOutlined from "@mui/icons-material/AddOutlined";
import DeleteOutline from "@mui/icons-material/DeleteOutline";
import EditOutlined from "@mui/icons-material/EditOutlined";
import ScienceOutlined from "@mui/icons-material/ScienceOutlined";
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  IconButton,
  Stack,
  Switch,
  Tooltip,
  Typography,
} from "@mui/material";
import type { GridColDef } from "@mui/x-data-grid";
import { useCallback, useMemo, useState } from "react";
import DataTable from "../../components/DataTable";
import {
  type JiraDryRunResult,
  type JiraTriggerDetail,
  deleteJiraTrigger,
  dryRunJiraTrigger,
  updateJiraTrigger,
} from "../../data/api";
import { useAuth } from "../../data/AuthContext";
import { useJiraSetup, useJiraTriggers } from "../../data/hooks";
import { MONO_SX } from "../../theme/hootFormat";
import JiraSetupPanel from "./JiraSetupPanel";
import RuleFormDialog from "./RuleFormDialog";

function flowSummary(row: JiraTriggerDetail): string {
  const when = row.fire_on;
  const project = row.project_key || "any project";
  const target =
    row.target_kind === "group"
      ? `group ${row.target_group_name ?? row.target_group_id ?? "?"}`
      : `room ${row.target_room_name ?? row.target_room_id ?? "?"}`;
  return `${project} · ${when} → ${target} → @${row.agent_name}`;
}

export default function JiraTriggersPage() {
  const { user } = useAuth();
  const isAdmin = user?.role === "admin";
  const { data: triggers, loading, error, refetch } = useJiraTriggers();
  const { data: setup, refetch: refetchSetup } = useJiraSetup();
  const [editorOpen, setEditorOpen] = useState(false);
  const [editing, setEditing] = useState<JiraTriggerDetail | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<JiraTriggerDetail | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [savingId, setSavingId] = useState<string | null>(null);
  const [dryRun, setDryRun] = useState<{
    rule: JiraTriggerDetail;
    result: JiraDryRunResult | null;
    loading: boolean;
    error: string | null;
  } | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const defaultInstance = setup?.instances[0]?.instance ?? "acme";

  const handleToggle = useCallback(
    async (row: JiraTriggerDetail, enabled: boolean) => {
      setSavingId(row.id);
      setActionError(null);
      try {
        await updateJiraTrigger(row.id, { enabled });
        refetch();
      } catch (err) {
        setActionError(err instanceof Error ? err.message : "Failed to update rule");
      } finally {
        setSavingId(null);
      }
    },
    [refetch],
  );

  const handleDelete = useCallback(async () => {
    if (!deleteTarget) return;
    setDeleting(true);
    setActionError(null);
    try {
      await deleteJiraTrigger(deleteTarget.id);
      setDeleteTarget(null);
      refetch();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Failed to delete rule");
    } finally {
      setDeleting(false);
    }
  }, [deleteTarget, refetch]);

  const handleDryRun = useCallback(async (row: JiraTriggerDetail) => {
    setDryRun({ rule: row, result: null, loading: true, error: null });
    try {
      const result = await dryRunJiraTrigger(row.id);
      setDryRun({ rule: row, result, loading: false, error: null });
    } catch (err) {
      setDryRun({
        rule: row,
        result: null,
        loading: false,
        error: err instanceof Error ? err.message : "Dry-run failed",
      });
    }
  }, []);

  const columns = useMemo<GridColDef<JiraTriggerDetail>[]>(
    () => [
      {
        field: "name",
        headerName: "Name",
        flex: 1,
        minWidth: 160,
      },
      {
        field: "project_key",
        headerName: "Project",
        width: 110,
        valueGetter: (_value, row) => row.project_key || "any",
      },
      {
        field: "flow",
        headerName: "When → target → agent",
        flex: 2,
        minWidth: 280,
        sortable: false,
        valueGetter: (_value, row) => flowSummary(row),
      },
      {
        field: "enabled",
        headerName: "Enabled",
        width: 110,
        sortable: false,
        renderCell: ({ row }) => (
          <Switch
            size="small"
            checked={row.enabled}
            disabled={!isAdmin || savingId === row.id}
            inputProps={{ "aria-label": `Enable rule ${row.name}` }}
            onChange={(e) => handleToggle(row, e.target.checked)}
          />
        ),
      },
      ...(isAdmin
        ? [
            {
              field: "actions" as const,
              headerName: "",
              width: 140,
              sortable: false,
              filterable: false,
              align: "right" as const,
              renderCell: ({ row }: { row: JiraTriggerDetail }) => (
                <Stack direction="row" spacing={0.5} justifyContent="flex-end">
                  <Tooltip title="Test rule (dry-run)">
                    <IconButton
                      size="small"
                      aria-label={`Test rule ${row.name}`}
                      onClick={() => handleDryRun(row)}
                    >
                      <ScienceOutlined fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Edit">
                    <IconButton
                      size="small"
                      aria-label={`Edit rule ${row.name}`}
                      onClick={() => {
                        setEditing(row);
                        setEditorOpen(true);
                      }}
                    >
                      <EditOutlined fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete">
                    <IconButton
                      size="small"
                      aria-label={`Delete rule ${row.name}`}
                      onClick={() => setDeleteTarget(row)}
                    >
                      <DeleteOutline fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </Stack>
              ),
            },
          ]
        : []),
    ],
    [handleDryRun, handleToggle, isAdmin, savingId],
  );

  const rows = useMemo(() => triggers ?? [], [triggers]);

  if (!isAdmin) {
    return (
      <Alert severity="info">Jira trigger configuration is available to admins.</Alert>
    );
  }

  return (
    <Box>
      <Stack direction="row" alignItems="center" mb={2}>
        <Typography variant="h5">Jira Triggers</Typography>
        <Button
          variant="contained"
          startIcon={<AddOutlined />}
          sx={{ ml: "auto" }}
          onClick={() => {
            setEditing(null);
            setEditorOpen(true);
          }}
        >
          New rule
        </Button>
      </Stack>

      <JiraSetupPanel
        setup={setup}
        onRotated={() => {
          refetchSetup();
        }}
      />

      {actionError && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setActionError(null)}>
          {actionError}
        </Alert>
      )}
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {loading ? (
        <CircularProgress />
      ) : rows.length === 0 ? (
        <Box
          sx={{
            border: "1px dashed var(--hoot-border)",
            borderRadius: 2,
            p: 4,
            textAlign: "center",
          }}
        >
          <Typography variant="h6" gutterBottom>
            No Jira trigger rules yet
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Create a rule to map Jira issue events to an addressed Switch room
            message. Configure the webhook above, then add your first rule.
          </Typography>
          <Button
            variant="contained"
            startIcon={<AddOutlined />}
            onClick={() => {
              setEditing(null);
              setEditorOpen(true);
            }}
          >
            New rule
          </Button>
        </Box>
      ) : (
        <DataTable rows={rows} columns={columns} />
      )}

      <RuleFormDialog
        open={editorOpen}
        trigger={editing}
        defaultInstance={defaultInstance}
        onClose={() => {
          setEditorOpen(false);
          setEditing(null);
        }}
        onSaved={() => refetch()}
      />

      <Dialog open={!!deleteTarget} onClose={() => setDeleteTarget(null)}>
        <DialogTitle>Delete trigger rule</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Delete &quot;{deleteTarget?.name}&quot;? This cannot be undone.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteTarget(null)}>Cancel</Button>
          <Button
            color="error"
            variant="contained"
            onClick={handleDelete}
            disabled={deleting}
            startIcon={deleting ? <CircularProgress size={16} /> : undefined}
          >
            Delete
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={!!dryRun}
        onClose={() => setDryRun(null)}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Dry-run: {dryRun?.rule.name}</DialogTitle>
        <DialogContent>
          {dryRun?.loading && <CircularProgress />}
          {dryRun?.error && <Alert severity="error">{dryRun.error}</Alert>}
          {dryRun?.result && (
            <Stack spacing={1.5} sx={{ mt: 1 }}>
              <Alert severity={dryRun.result.matched ? "success" : "warning"}>
                {dryRun.result.matched
                  ? "Rule matched the sample event."
                  : "Rule did not match the sample event."}
              </Alert>
              <Typography variant="body2" color="text.secondary">
                This preview does not post to any room.
              </Typography>
              <Typography variant="subtitle2">Why</Typography>
              <Box component="ul" sx={{ m: 0, pl: 2 }}>
                {dryRun.result.reasons.map((r) => (
                  <Typography component="li" variant="body2" key={r}>
                    {r}
                  </Typography>
                ))}
              </Box>
              {dryRun.result.rendered_message != null && (
                <>
                  <Typography variant="subtitle2">Rendered message</Typography>
                  <Typography
                    variant="body2"
                    sx={{
                      ...MONO_SX,
                      whiteSpace: "pre-wrap",
                      p: 1.5,
                      borderRadius: 1,
                      bgcolor: "action.hover",
                    }}
                  >
                    {dryRun.result.rendered_message}
                  </Typography>
                </>
              )}
              {dryRun.result.targets.length > 0 && (
                <>
                  <Typography variant="subtitle2">Targets</Typography>
                  {dryRun.result.targets.map((t) => (
                    <Typography variant="body2" key={t.room_id}>
                      {t.room_name ?? t.room_id} → @{t.agent_name}
                      {t.group_name ? ` (group ${t.group_name})` : ""}
                    </Typography>
                  ))}
                </>
              )}
            </Stack>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDryRun(null)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
