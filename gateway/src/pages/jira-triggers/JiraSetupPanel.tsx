import ContentCopy from "@mui/icons-material/ContentCopy";
import DoneOutline from "@mui/icons-material/DoneOutline";
import RefreshOutlined from "@mui/icons-material/RefreshOutlined";
import VisibilityOutlined from "@mui/icons-material/VisibilityOutlined";
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  IconButton,
  Stack,
  Tooltip,
  Typography,
} from "@mui/material";
import { useCallback, useState } from "react";
import {
  type JiraSetupInfo,
  revealJiraInstanceSecret,
  rotateJiraInstanceSecret,
} from "../../data/api";
import { MONO_SX } from "../../theme/hootFormat";

async function copyText(value: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }
  const ta = document.createElement("textarea");
  ta.value = value;
  ta.style.position = "fixed";
  ta.style.opacity = "0";
  document.body.appendChild(ta);
  ta.select();
  document.execCommand("copy");
  document.body.removeChild(ta);
}

export default function JiraSetupPanel({
  setup,
  onRotated,
}: {
  setup: JiraSetupInfo | null;
  onRotated: () => void;
}) {
  const [busy, setBusy] = useState<string | null>(null);
  const [copied, setCopied] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [rotatedNote, setRotatedNote] = useState<string | null>(null);

  const markCopied = useCallback((key: string) => {
    setCopied(key);
    setTimeout(() => setCopied(null), 2000);
  }, []);

  const handleCopyUrl = useCallback(
    async (instance: string, url: string) => {
      await copyText(url);
      markCopied(`url:${instance}`);
    },
    [markCopied],
  );

  const handleRevealCopy = useCallback(
    async (instance: string) => {
      setBusy(`reveal:${instance}`);
      setError(null);
      try {
        const secret = await revealJiraInstanceSecret(instance);
        await copyText(secret);
        markCopied(`secret:${instance}`);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to reveal secret");
      } finally {
        setBusy(null);
      }
    },
    [markCopied],
  );

  const handleRotate = useCallback(
    async (instance: string) => {
      setBusy(`rotate:${instance}`);
      setError(null);
      setRotatedNote(null);
      try {
        const result = await rotateJiraInstanceSecret(instance);
        await copyText(result.secret);
        setRotatedNote(result.note);
        markCopied(`secret:${instance}`);
        onRotated();
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to rotate secret");
      } finally {
        setBusy(null);
      }
    },
    [markCopied, onRotated],
  );

  if (!setup) {
    return (
      <Alert severity="info">
        Load setup details to see webhook URLs and secrets. Configure{" "}
        <Typography component="span" sx={MONO_SX}>
          JIRA_WEBHOOK_SECRETS
        </Typography>{" "}
        on the server first.
      </Alert>
    );
  }

  return (
    <Stack spacing={2} sx={{ mb: 3 }}>
      <Typography variant="h6">Webhook setup</Typography>
      <Typography variant="body2" color="text.secondary">
        {setup.guidance.room_membership}
      </Typography>
      <Typography variant="body2" color="text.secondary">
        {setup.guidance.classic_webhook}
      </Typography>
      <Typography variant="body2" color="text.secondary">
        {setup.guidance.automation}
      </Typography>

      {setup.instances.length === 0 ? (
        <Alert severity="warning">
          No Jira instances are configured. Set{" "}
          <Typography component="span" sx={MONO_SX}>
            JIRA_WEBHOOK_SECRETS
          </Typography>{" "}
          (for example{" "}
          <Typography component="span" sx={MONO_SX}>
            {`{"acme":"…"}`}
          </Typography>
          ) and restart Switch.
        </Alert>
      ) : (
        setup.instances.map((inst) => (
          <Box
            key={inst.instance}
            sx={{
              border: "1px solid var(--hoot-border)",
              borderRadius: 2,
              p: 2,
            }}
          >
            <Typography variant="subtitle2" sx={{ mb: 1 }}>
              Instance{" "}
              <Typography component="span" sx={MONO_SX}>
                {inst.instance}
              </Typography>
            </Typography>
            <Stack spacing={1}>
              <Stack direction="row" alignItems="center" spacing={1}>
                <Typography variant="body2" sx={{ ...MONO_SX, flex: 1 }} noWrap>
                  {inst.webhook_url}
                </Typography>
                <Tooltip
                  title={
                    copied === `url:${inst.instance}` ? "Copied!" : "Copy webhook URL"
                  }
                >
                  <IconButton
                    size="small"
                    aria-label={`Copy webhook URL for ${inst.instance}`}
                    onClick={() => handleCopyUrl(inst.instance, inst.webhook_url)}
                  >
                    {copied === `url:${inst.instance}` ? (
                      <DoneOutline fontSize="small" color="success" />
                    ) : (
                      <ContentCopy fontSize="small" />
                    )}
                  </IconButton>
                </Tooltip>
              </Stack>
              <Stack direction="row" alignItems="center" spacing={1}>
                <Typography variant="body2" color="text.secondary">
                  Secret
                </Typography>
                <Typography variant="body2" sx={{ ...MONO_SX, flex: 1 }}>
                  {inst.secret_masked}
                </Typography>
                <Tooltip
                  title={
                    copied === `secret:${inst.instance}`
                      ? "Copied!"
                      : "Reveal and copy secret"
                  }
                >
                  <span>
                    <IconButton
                      size="small"
                      aria-label={`Reveal and copy secret for ${inst.instance}`}
                      disabled={busy === `reveal:${inst.instance}`}
                      onClick={() => handleRevealCopy(inst.instance)}
                    >
                      {busy === `reveal:${inst.instance}` ? (
                        <CircularProgress size={16} />
                      ) : copied === `secret:${inst.instance}` ? (
                        <DoneOutline fontSize="small" color="success" />
                      ) : (
                        <VisibilityOutlined fontSize="small" />
                      )}
                    </IconButton>
                  </span>
                </Tooltip>
                <Button
                  size="small"
                  startIcon={
                    busy === `rotate:${inst.instance}` ? (
                      <CircularProgress size={14} />
                    ) : (
                      <RefreshOutlined />
                    )
                  }
                  disabled={busy === `rotate:${inst.instance}`}
                  onClick={() => handleRotate(inst.instance)}
                >
                  Rotate
                </Button>
              </Stack>
            </Stack>
          </Box>
        ))
      )}

      {rotatedNote && <Alert severity="warning">{rotatedNote}</Alert>}
      {error && <Alert severity="error">{error}</Alert>}
    </Stack>
  );
}
