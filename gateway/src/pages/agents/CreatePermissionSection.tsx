import {
  Alert,
  Box,
  Button,
  CircularProgress,
  FormControlLabel,
  Switch,
  Typography,
} from "@mui/material";
import { useEffect, useState } from "react";
import { type AgentDetail, updateAgentCreatePermission } from "../../data/api";

export default function CreatePermissionSection({
  agent,
  canEdit,
  onUpdated,
}: {
  agent: AgentDetail;
  canEdit: boolean;
  onUpdated: () => void;
}) {
  const [granted, setGranted] = useState(agent.can_create_agents);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setGranted(agent.can_create_agents);
    setError(null);
  }, [agent.id, agent.can_create_agents]);

  const dirty = granted !== agent.can_create_agents;

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      await updateAgentCreatePermission(agent.id, granted);
      onUpdated();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save permission");
    } finally {
      setSaving(false);
    }
  };

  return (
    <Box>
      <Typography variant="overline" sx={{ color: "text.secondary", display: "block" }}>
        Agent creation
      </Typography>
      <Typography variant="caption" color="text.secondary">
        Grants this agent permission to create new agents (owned by the same
        owner) via the create_agent tool. Off unless a human turns it on.
      </Typography>

      <Box sx={{ mt: 1 }}>
        <FormControlLabel
          disabled={!canEdit}
          control={
            <Switch
              size="small"
              checked={granted}
              onChange={(e) => setGranted(e.target.checked)}
            />
          }
          label="May create agents"
        />
      </Box>
      {!canEdit && (
        <Typography variant="body2" color="text.secondary">
          Only the agent's owner can change this permission.
        </Typography>
      )}

      {error && (
        <Alert severity="error" sx={{ mt: 1 }}>
          {error}
        </Alert>
      )}

      {canEdit && (
        <Box sx={{ mt: 1 }}>
          <Button
            variant="contained"
            disabled={!dirty || saving}
            onClick={handleSave}
            startIcon={saving ? <CircularProgress size={16} /> : undefined}
          >
            Save permission
          </Button>
        </Box>
      )}
    </Box>
  );
}
