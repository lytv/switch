import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Location } from '@shared/core/locations/locations';

const LOCATION: Location = {
  id: 'loc-1',
  name: 'repo',
  dir: '/repo',
  sshHost: null,
  createdAt: '2024-01-01',
  updatedAt: '2024-01-01',
};

const mockCheckIsValidDirectory = vi.fn();
const mockEnsureLocation = vi.fn();
const mockOpenLocation = vi.fn();
const mockReconcileLocation = vi.fn();

vi.mock('./path-utils', () => ({ checkIsValidDirectory: mockCheckIsValidDirectory }));
vi.mock('./store', () => ({ ensureLocation: mockEnsureLocation }));
vi.mock('./location-manager', () => ({
  locationManager: { openLocation: mockOpenLocation },
}));
vi.mock('@main/core/agents/configured-agent-discovery-service', () => ({
  configuredAgentDiscoveryService: { reconcileLocation: mockReconcileLocation },
}));

const { openLocationFolder } = await import('./open-location');

beforeEach(() => {
  vi.clearAllMocks();
  mockCheckIsValidDirectory.mockReturnValue(true);
  mockEnsureLocation.mockResolvedValue(LOCATION);
  mockOpenLocation.mockResolvedValue({ success: true, data: {} });
  mockReconcileLocation.mockResolvedValue(undefined);
});

describe('openLocationFolder', () => {
  it('rejects a directory that does not exist', async () => {
    mockCheckIsValidDirectory.mockReturnValue(false);
    const result = await openLocationFolder('/nope');
    expect(result).toEqual({ success: false, error: { type: 'invalid-directory', dir: '/nope' } });
    expect(mockEnsureLocation).not.toHaveBeenCalled();
  });

  it('ensures the location, opens it, and adopts configured agents', async () => {
    const result = await openLocationFolder('/repo');
    expect(result).toEqual({ success: true, data: LOCATION });
    expect(mockEnsureLocation).toHaveBeenCalledWith({ sshHost: null, dir: '/repo', name: 'repo' });
    expect(mockOpenLocation).toHaveBeenCalledWith(LOCATION);
    expect(mockReconcileLocation).toHaveBeenCalledWith(LOCATION);
  });

  it('is a no-op success on a second call for the same folder', async () => {
    const first = await openLocationFolder('/repo');
    const second = await openLocationFolder('/repo');
    expect(first).toEqual(second);
    expect(mockEnsureLocation).toHaveBeenCalledTimes(2);
    expect(mockOpenLocation).toHaveBeenCalledTimes(2);
    expect(mockReconcileLocation).toHaveBeenCalledTimes(2);
  });
});
