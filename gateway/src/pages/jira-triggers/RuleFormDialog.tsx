import {
  Alert,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormControlLabel,
  FormHelperText,
  InputLabel,
  MenuItem,
  Radio,
  RadioGroup,
  Select,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import { useCallback, useEffect, useMemo, useState } from "react";
import SearchablePickerDialog, {
  type PickerOption,
} from "../../components/SearchablePickerDialog";
import {
  type JiraTriggerDetail,
  type JiraTriggerInput,
  createJiraTrigger,
  fetchJiraAgentOptions,
  fetchJiraMessageTokens,
  updateJiraTrigger,
} from "../../data/api";
import { useRoomGroups, useRooms } from "../../data/hooks";

const FIRE_ON = ["created", "updated", "transition"] as const;
const ISSUE_TYPES = ["", "Story", "Bug", "Task", "Epic", "Sub-task"];

type FormState = {
  name: string;
  instance: string;
  project_key: string;
  issue_type: string;
  fire_on: string;
  target_status: string;
  jql: string;
  target_kind: "room" | "group";
  target_room_id: string;
  target_group_id: string;
  agent_name: string;
  message_template: string;
  thread_by: "new" | "issue_key";
};

function emptyForm(defaultInstance: string): FormState {
  return {
    name: "",
    instance: defaultInstance,
    project_key: "",
    issue_type: "",
    fire_on: "transition",
    target_status: "",
    jql: "",
    target_kind: "room",
    target_room_id: "",
    target_group_id: "",
    agent_name: "",
    message_template:
      "{{issue.key}} {{issue.summary}} is now {{issue.status}}",
    thread_by: "new",
  };
}

function fromTrigger(trigger: JiraTriggerDetail): FormState {
  return {
    name: trigger.name,
    instance: trigger.instance,
    project_key: trigger.project_key,
    issue_type: trigger.issue_type,
    fire_on: trigger.fire_on,
    target_status: trigger.target_status,
    jql: trigger.jql,
    target_kind: trigger.target_kind === "group" ? "group" : "room",
    target_room_id: trigger.target_room_id ?? "",
    target_group_id: trigger.target_group_id ?? "",
    agent_name: trigger.agent_name,
    message_template: trigger.message_template,
    thread_by: trigger.thread_by === "issue_key" ? "issue_key" : "new",
  };
}

export default function RuleFormDialog({
  open,
  trigger,
  defaultInstance,
  onClose,
  onSaved,
}: {
  open: boolean;
  trigger: JiraTriggerDetail | null;
  defaultInstance: string;
  onClose: () => void;
  onSaved: () => void;
}) {
  const editing = trigger !== null;
  const { data: rooms } = useRooms();
  const { data: groups } = useRoomGroups();
  const [form, setForm] = useState<FormState>(() => emptyForm(defaultInstance));
  const [tokens, setTokens] = useState<string[]>([]);
  const [agents, setAgents] = useState<{ id: string; name: string }[]>([]);
  const [agentsLoading, setAgentsLoading] = useState(false);
  const [picker, setPicker] = useState<"room" | "group" | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});

  const formKey = `${open}:${trigger?.id ?? "new"}:${defaultInstance}`;
  const [lastKey, setLastKey] = useState(formKey);
  if (formKey !== lastKey) {
    setLastKey(formKey);
    setForm(trigger ? fromTrigger(trigger) : emptyForm(defaultInstance));
    setError(null);
    setFieldErrors({});
  }

  useEffect(() => {
    if (!open) return;
    fetchJiraMessageTokens().then((list) => {
      if (list) setTokens(list);
    });
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const roomId = form.target_kind === "room" ? form.target_room_id : undefined;
    const groupId = form.target_kind === "group" ? form.target_group_id : undefined;
    if (!roomId && !groupId) {
      setAgents([]);
      return;
    }
    let cancelled = false;
    setAgentsLoading(true);
    fetchJiraAgentOptions({ roomId, groupId }).then((list) => {
      if (cancelled) return;
      setAgents(list ?? []);
      setAgentsLoading(false);
    });
    return () => {
      cancelled = true;
    };
  }, [open, form.target_kind, form.target_room_id, form.target_group_id]);

  const roomLabel = useMemo(() => {
    if (!form.target_room_id) return "Choose a room";
    return (
      rooms?.find((r) => r.id === form.target_room_id)?.name ??
      trigger?.target_room_name ??
      form.target_room_id
    );
  }, [form.target_room_id, rooms, trigger]);

  const groupLabel = useMemo(() => {
    if (!form.target_group_id) return "Choose a group";
    return (
      groups?.find((g) => g.id === form.target_group_id)?.name ??
      trigger?.target_group_name ??
      form.target_group_id
    );
  }, [form.target_group_id, groups, trigger]);

  const roomOptions: PickerOption[] = useMemo(
    () =>
      (rooms ?? []).map((r) => ({
        id: r.id,
        primary: r.name,
        secondary: r.description || undefined,
        search: `${r.name} ${r.description ?? ""}`.toLowerCase(),
      })),
    [rooms],
  );

  const groupOptions: PickerOption[] = useMemo(
    () =>
      (groups ?? []).map((g) => ({
        id: g.id,
        primary: g.name,
        secondary: g.description || undefined,
        search: `${g.name} ${g.description ?? ""}`.toLowerCase(),
      })),
    [groups],
  );

  const setField = useCallback(<K extends keyof FormState>(key: K, value: FormState[K]) => {
    setForm((prev) => ({ ...prev, [key]: value }));
    setFieldErrors((prev) => {
      if (!prev[key]) return prev;
      const next = { ...prev };
      delete next[key];
      return next;
    });
  }, []);

  const validate = useCallback((): boolean => {
    const next: Record<string, string> = {};
    if (!form.name.trim()) next.name = "Name is required";
    if (!form.instance.trim()) next.instance = "Instance is required";
    if (!form.message_template.trim()) next.message_template = "Message template is required";
    if (form.target_kind === "room" && !form.target_room_id) {
      next.target_room_id = "Choose a room";
    }
    if (form.target_kind === "group" && !form.target_group_id) {
      next.target_group_id = "Choose a group";
    }
    if (!form.agent_name.trim()) next.agent_name = "Choose an agent";
    setFieldErrors(next);
    return Object.keys(next).length === 0;
  }, [form]);

  const handleSave = useCallback(async () => {
    if (!validate()) return;
    setBusy(true);
    setError(null);
    const payload: JiraTriggerInput = {
      name: form.name.trim(),
      instance: form.instance.trim(),
      project_key: form.project_key.trim(),
      issue_type: form.issue_type.trim(),
      fire_on: form.fire_on,
      target_status: form.target_status.trim(),
      jql: form.jql.trim(),
      target_kind: form.target_kind,
      target_room_id: form.target_kind === "room" ? form.target_room_id : null,
      target_group_id: form.target_kind === "group" ? form.target_group_id : null,
      agent_name: form.agent_name.trim(),
      message_template: form.message_template,
      thread_by: form.thread_by,
    };
    try {
      if (editing && trigger) {
        await updateJiraTrigger(trigger.id, payload);
      } else {
        await createJiraTrigger(payload);
      }
      onSaved();
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save rule");
    } finally {
      setBusy(false);
    }
  }, [editing, form, onClose, onSaved, trigger, validate]);

  const insertToken = useCallback((token: string) => {
    setForm((prev) => ({
      ...prev,
      message_template: `${prev.message_template}{{${token}}}`,
    }));
  }, []);

  return (
    <>
      <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
        <DialogTitle>{editing ? "Edit rule" : "New rule"}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              label="Name"
              value={form.name}
              onChange={(e) => setField("name", e.target.value)}
              error={Boolean(fieldErrors.name)}
              helperText={fieldErrors.name}
              required
              fullWidth
            />
            <TextField
              label="Instance"
              value={form.instance}
              onChange={(e) => setField("instance", e.target.value)}
              error={Boolean(fieldErrors.instance)}
              helperText={
                fieldErrors.instance ??
                "Must match the /integrations/jira/{instance} path segment"
              }
              required
              fullWidth
            />
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Project key"
                value={form.project_key}
                onChange={(e) => setField("project_key", e.target.value)}
                helperText="Blank = any project"
                fullWidth
              />
              <FormControl fullWidth>
                <InputLabel id="jira-issue-type-label">Issue type</InputLabel>
                <Select
                  labelId="jira-issue-type-label"
                  label="Issue type"
                  value={form.issue_type}
                  onChange={(e) => setField("issue_type", e.target.value)}
                >
                  {ISSUE_TYPES.map((t) => (
                    <MenuItem key={t || "any"} value={t}>
                      {t || "Any"}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Stack>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <FormControl fullWidth>
                <InputLabel id="jira-fire-on-label">Fire on</InputLabel>
                <Select
                  labelId="jira-fire-on-label"
                  label="Fire on"
                  value={form.fire_on}
                  onChange={(e) => setField("fire_on", e.target.value)}
                >
                  {FIRE_ON.map((v) => (
                    <MenuItem key={v} value={v}>
                      {v}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <TextField
                label="Target status"
                value={form.target_status}
                onChange={(e) => setField("target_status", e.target.value)}
                helperText={
                  form.fire_on === "transition"
                    ? "Blank = any transition"
                    : "Optional current-status filter"
                }
                fullWidth
              />
            </Stack>
            <TextField
              label="JQL filter"
              value={form.jql}
              onChange={(e) => setField("jql", e.target.value)}
              helperText='Optional simple filter, e.g. priority = High AND labels in (agentic)'
              fullWidth
            />

            <FormControl error={Boolean(fieldErrors.target_room_id || fieldErrors.target_group_id)}>
              <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
                Target
              </Typography>
              <RadioGroup
                row
                value={form.target_kind}
                onChange={(e) => {
                  const kind = e.target.value as "room" | "group";
                  setForm((prev) => ({
                    ...prev,
                    target_kind: kind,
                    agent_name: "",
                  }));
                }}
              >
                <FormControlLabel value="room" control={<Radio />} label="Room" />
                <FormControlLabel value="group" control={<Radio />} label="Group" />
              </RadioGroup>
              {form.target_kind === "room" ? (
                <Button
                  variant="outlined"
                  onClick={() => setPicker("room")}
                  sx={{ alignSelf: "flex-start" }}
                >
                  {roomLabel}
                </Button>
              ) : (
                <Button
                  variant="outlined"
                  onClick={() => setPicker("group")}
                  sx={{ alignSelf: "flex-start" }}
                >
                  {groupLabel}
                </Button>
              )}
              <FormHelperText>
                {fieldErrors.target_room_id || fieldErrors.target_group_id}
              </FormHelperText>
            </FormControl>

            <FormControl fullWidth error={Boolean(fieldErrors.agent_name)}>
              <InputLabel id="jira-agent-label">Agent</InputLabel>
              <Select
                labelId="jira-agent-label"
                label="Agent"
                value={form.agent_name}
                onChange={(e) => setField("agent_name", e.target.value)}
                disabled={agentsLoading || agents.length === 0}
              >
                {agents.map((a) => (
                  <MenuItem key={a.id} value={a.name}>
                    {a.name}
                  </MenuItem>
                ))}
              </Select>
              <FormHelperText>
                {fieldErrors.agent_name ||
                  (agentsLoading
                    ? "Loading agents…"
                    : "Limited to members of the chosen room or group")}
              </FormHelperText>
            </FormControl>

            <FormControl>
              <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
                Threading
              </Typography>
              <RadioGroup
                row
                value={form.thread_by}
                onChange={(e) =>
                  setField("thread_by", e.target.value as "new" | "issue_key")
                }
              >
                <FormControlLabel
                  value="new"
                  control={<Radio />}
                  label="New message each time"
                />
                <FormControlLabel
                  value="issue_key"
                  control={<Radio />}
                  label="Thread by issue key"
                />
              </RadioGroup>
            </FormControl>

            <TextField
              label="Message template"
              value={form.message_template}
              onChange={(e) => setField("message_template", e.target.value)}
              error={Boolean(fieldErrors.message_template)}
              helperText={fieldErrors.message_template}
              required
              fullWidth
              multiline
              minRows={3}
            />
            <Stack direction="row" flexWrap="wrap" gap={1}>
              {tokens.map((token) => (
                <Chip
                  key={token}
                  size="small"
                  label={`{{${token}}}`}
                  onClick={() => insertToken(token)}
                  clickable
                />
              ))}
            </Stack>

            {error && <Alert severity="error">{error}</Alert>}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={onClose}>Cancel</Button>
          <Button
            variant="contained"
            onClick={handleSave}
            disabled={busy}
            startIcon={busy ? <CircularProgress size={16} /> : undefined}
          >
            Save
          </Button>
        </DialogActions>
      </Dialog>

      <SearchablePickerDialog
        open={picker === "room"}
        title="Choose a room"
        options={roomOptions}
        singleSelect
        submitLabel="Select"
        onClose={() => setPicker(null)}
        onSubmit={(ids) => {
          if (ids[0]) {
            setForm((prev) => ({
              ...prev,
              target_room_id: ids[0],
              agent_name: "",
            }));
          }
          setPicker(null);
        }}
      />
      <SearchablePickerDialog
        open={picker === "group"}
        title="Choose a group"
        options={groupOptions}
        singleSelect
        submitLabel="Select"
        onClose={() => setPicker(null)}
        onSubmit={(ids) => {
          if (ids[0]) {
            setForm((prev) => ({
              ...prev,
              target_group_id: ids[0],
              agent_name: "",
            }));
          }
          setPicker(null);
        }}
      />
    </>
  );
}
