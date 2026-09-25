# Code Review Report: `groupIdentity=Hash` Deployment Mode in LeaderWorkerSet (`kubernetes-sigs/lws`)

- **Repository**: [`/usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws)
- **Reviewed Branch / Commit**: `hash-mode` (`d4f1525` — `docs: add Group Identity concept page (#1062)`)
- **Design Reference**: [KEP-898: Hash Group Identity](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/keps/898-group-identity/README.md)

---

## 1. Executive Summary

In `groupIdentity: Hash` mode, `LeaderWorkerSet` manages leader pods via a `Deployment` (and its underlying `ReplicaSet`s) rather than a leader `StatefulSet`, while stamping each leader pod at mutating admission with:
- A random 40-character SHA-1 group key in `leaderworkerset.sigs.k8s.io/group-index` and `leaderworkerset.sigs.k8s.io/group-key` ([`PodWebhook.Default`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L105-L123)).
- A deterministic DNS hostname `<lwsName>-<8-char-key-prefix>` ([`hashLeaderHostname`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L238-L244)).
- A `leaderworkerset.sigs.k8s.io/group-ready` readiness gate (when `size > 1`) so Deployment rollout budgets and `ReplicaSet` scale-down victim ranking operate on whole-group readiness ([`constructLeaderDeploymentApplyConfiguration`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L181-L184), [`syncGroupReadyCondition`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L353-L376)).
- A `leaderworkerset.sigs.k8s.io/group-replacement` scheduling gate lifted by [`reconcileGroupReplacementGate`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L749-L797) to serialize group replacements under `groupReplacementPolicy: PostTermination`.

This review identifies **9 bugs** (2 Critical, 4 High, 3 Medium) across the controller, webhooks, TPU environment injection, workload-aware scheduling providers, and `DisaggregatedSet` integration.

### Summary Table

| # | Severity | Area | Summary |
| :- | :- | :- | :- |
| 1 | 🔴 **Critical** | `leaderworkerset_hash.go` | `updateStatusHash` ignores `deploy.Status.ObservedGeneration < deploy.Generation`, reporting `Available=True` (`updateDone=true`) immediately on template update and prematurely deleting active `ControllerRevision`s via `TruncateRevisions`. |
| 2 | 🔴 **Critical** | `tpu.go` / `pod_webhook.go` | `AddTPUVariables` and `addTPUVariablesSubGroup` use `pod.Name` (which is `""` at mutating admission for `generateName` hash leaders), injecting broken `TPU_WORKER_HOSTNAMES` (`"-1.<subdomain>"`), `TPU_PROCESS_ADDRESSES`, and `TPU_NAME=""` on leader pods. |
| 3 | 🔴 **High** | `pod_webhook.go` | `PodWebhook.Default` regenerates `groupUniqueKey` unconditionally on webhook reinvocation while keeping `Hostname`, `Subdomain`, `ExclusiveAffinities`, and `SubGroupUniqueHashLabelKey` bound to the first key, causing a self-anti-affinity scheduling deadlock. |
| 4 | 🔴 **High** | `disaggregatedset_webhook.go` / `leaderworkerset_webhook.go` | Unvalidated 63-char limit overflow: hash mode adds a 28-char suffix (`-<10charRS>-<5charPod>-<10charStsRev>`) to `lws.Name` on worker `controller-revision-hash` labels (`len(lws.Name) > 35` fails worker pod creation) and a 9-char suffix on leader `hostname` (`len(lws.Name) > 54` fails leader pod creation). `DisaggregatedSetWebhook` validates hash roles using ordinal math (`13` chars instead of `28`). |
| 5 | 🔴 **High** | `pod_controller.go` | `reconcileGroupReplacementGate` (`rank < len(gated) - terminating`) blocks replacement groups behind unrelated scaled-down groups, loses `maxSurge` slots once a surge pod leaves `gated`, and emits spurious `GroupReplacementDeferred` events due to queue ordering and per-pod delete fan-out. |
| 6 | 🔴 **High** | `volcano_provider.go` / `kubernetes_provider.go` | `VolcanoProvider` with `spec.scheduling` sets `ControllerReference` of per-group `PodGroup`s to `lws` instead of `leaderPod` with no cleanup logic, leaking Volcano `PodGroup`s on every group restart/scale-down/rollout; `KubernetesProvider` misses `PodGroup` cleanup when worker pods terminate after background leader deletion. |
| 7 | 🟡 **Medium** | `leaderworkerset_hash.go` | `updateStatusHash` leaves `UpdateInProgress=True` stuck when transitioning to non-update `Progressing`, fails to set `UpdateInProgress=True` on single-replica / final-step rollouts once the old leader terminates, and never sets the `Degraded` condition. |
| 8 | 🟡 **Medium** | `pod_controller.go` | `PodReconciler.reconcilePod` reads mutable `SubdomainPolicy` and `Size` from the live `LeaderWorkerSet` rather than the leader pod's `ControllerRevision`, breaking old-revision groups during rolling updates that toggle `SubdomainPolicy` or `Size`. |
| 9 | 🟡 **Medium** | `disaggregatedset_controller.go` / `executor.go` | `DisaggregatedSet` only calls `SyncGroupReplacementPolicy` in `reconcileRoleSimple` and never during `ReconcileRollingUpdateNew`, ignoring `groupReplacementPolicy` updates mid-rollout; `PostTermination` also does not serialize group replacement across `DisaggregatedSet` revisions. |

---

## 2. Detailed Findings

### 🔴 1. [Critical] Premature `TruncateRevisions` and False `Available` Status on Template Rollout (`updateStatusHash` Ignores `deploy.Status.ObservedGeneration`)

**Locations:**
- [`pkg/controllers/leaderworkerset_hash.go`: `reconcileHash` (L92–L121)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L92-L121)
- [`pkg/controllers/leaderworkerset_hash.go`: `updateStatusHash` (L211–L272)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L211-L272)

#### Reproduction Trace
Suppose a `groupIdentity: Hash` `LeaderWorkerSet` with `replicas: 3` is running and healthy on revision `rev-1` (`deploy.Generation == 1`, `deploy.Status = {ObservedGeneration: 1, Replicas: 3, ReadyReplicas: 3, UpdatedReplicas: 3}`).
Now a user updates `spec.leaderWorkerTemplate`:
1. [`reconcileHash`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L67-L80) creates a new `ControllerRevision` (`rev-2`).
2. [`r.SSAWithDeployment(ctx, lws, "rev-2")`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L92) patches `deploy.Spec.Template` with `rev-2`, incrementing `deploy.Generation` to `2` on the API server.
3. Immediately on [L110](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L110)—in the same reconcile call, before `kube-controller-manager`'s Deployment controller has observed `deploy.Generation == 2` and recomputed `deploy.Status`—`reconcileHash` calls [`r.updateStatusHash(ctx, lws)`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L211-L272).
4. `updateStatusHash` fetches `deploy` (whose `deploy.Status.ObservedGeneration` is still `1` with `Replicas: 3, ReadyReplicas: 3, UpdatedReplicas: 3` from `rev-1`) and computes ([L246–L249](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L246-L249)):
   ```go
   updateInProgress := deploy.Status.UpdatedReplicas < deploy.Status.Replicas
   available := deploy.Status.Replicas == lwsReplicas &&
       deploy.Status.ReadyReplicas == lwsReplicas &&
       deploy.Status.UpdatedReplicas == lwsReplicas
   ```
   Because `updateStatusHash` does not check `deploy.Status.ObservedGeneration >= deploy.Generation`, `available` evaluates to `true` (`3 == 3`) and `updateStatusHash` returns `updateDone = true`.
5. [Lines 117–120](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L117-L120) then execute immediately:
   ```go
   if updateDone {
       if err := revisionutils.TruncateRevisions(ctx, r.Client, lws, revisionutils.GetRevisionKey(revision)); err != nil {
           return ctrl.Result{}, err
       }
   }
   ```
   **`TruncateRevisions` deletes `rev-1` while 100% of the running pods are still on `rev-1`!**

#### Impact
- **Broken `PodReconciler` for all running `rev-1` pods**: Whenever [`PodReconciler.reconcilePod`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L266-L274) reconciles a `rev-1` leader pod during the rollout, `revisionutils.GetRevision` returns `nil`, causing `reconcilePod` to return early (`RequeueAfter: 1s`) before reaching [`syncGroupReadyCondition`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L342) or creating worker `StatefulSet`s.
- **False rollout completion signal**: `lws.Status.ObservedGeneration` is immediately set to the new `lws.Generation` with `Available = True` and `UpdateInProgress = False` before any pod has rolled.

#### Recommended Fix
Check `deploy.Status.ObservedGeneration >= deploy.Generation && revisionutils.GetRevisionKey(deploy) == revisionKey` in `updateStatusHash`:
```go
deploymentObserved := deploy.Status.ObservedGeneration >= deploy.Generation &&
	revisionutils.GetRevisionKey(deploy) == revisionKey
updateInProgress := !deploymentObserved || deploy.Status.UpdatedReplicas < deploy.Status.Replicas
available := deploymentObserved &&
	deploy.Status.Replicas == lwsReplicas &&
	deploy.Status.ReadyReplicas == lwsReplicas &&
	deploy.Status.UpdatedReplicas == lwsReplicas
```

---

### 🔴 2. [Critical] Broken TPU Environment Variables (`TPU_WORKER_HOSTNAMES`, `TPU_PROCESS_ADDRESSES`, `TPU_NAME`) on Hash-Mode Leader Pods

**Locations:**
- [`pkg/webhooks/pod_webhook.go`: `PodWebhook.Default` (L103–L200)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L103-L200)
- [`pkg/utils/accelerators/tpu.go`: `addTPUVariablesSubGroup` (L113–L215)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/utils/accelerators/tpu.go#L113-L215)
- [`pkg/utils/accelerators/tpu.go`: `AddTPUVariables` (L218–L316)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/utils/accelerators/tpu.go#L218-L316)

#### Reproduction Trace
1. In `groupIdentity: Hash` mode, leader pods are created by a `ReplicaSet` using `metadata.generateName`. In the Kubernetes API server admission pipeline, `NameGenerator.GenerateName` runs in `rest.BeforeCreate` **after** mutating admission webhooks run. Thus, `pod.Name` is `""` during [`PodWebhook.Default`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L106-L108).
2. At [L197–L200](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L197-L200), `PodWebhook.Default` calls `acceleratorutils.AddTPUVariables(pod, podCount)`.
3. In [`AddTPUVariables`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/utils/accelerators/tpu.go#L239-L243) (and [`addTPUVariablesSubGroup`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/utils/accelerators/tpu.go#L126)), when `pod.Labels[WorkerIndexLabelKey] == "0"`, `leaderPodName` is set to `pod.Name` (`""`).
4. While the leader's own entry uses `leaderDNSHostname(pod, leaderPodName)` (`pod.Spec.Hostname`), the loop generating worker hostnames ([L278–L284](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/utils/accelerators/tpu.go#L278-L284) and [L184–L187](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/utils/accelerators/tpu.go#L184-L187)) executes:
   ```go
   for i := 1; i <= size-1; i++ {
       podHostname := fmt.Sprintf("%s-%d.%s", leaderPodName, i, pod.Spec.Subdomain)
       ...
   }
   ```
   With `leaderPodName == ""`, the leader pod gets `"-1.<subdomain>,-2.<subdomain>"` in `TPU_WORKER_HOSTNAMES` and `TPU_PROCESS_ADDRESSES`, and `TPU_NAME` ([L301](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/utils/accelerators/tpu.go#L301) / [L200](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/utils/accelerators/tpu.go#L200)) is set to `""`.
5. Furthermore, when `PodReconciler` later creates the worker `StatefulSet`, it names the worker `StatefulSet` after `leaderPod.Name` ([`pod_controller.go:1040`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L1040)), so the actual worker pods receive DNS names `<leaderPod.Name>-1.<subdomain>`, which was not even generated yet when the leader pod's mutating webhook ran.

#### Recommended Fix
Either name the worker `StatefulSet` in hash mode after `leaderPod.Spec.Hostname` (`<lwsName>-<8charKey>`, which is known at leader admission time) and use `leaderDNSHostname(pod, leaderPodName)` as the prefix for both leader and worker TPU hostnames (`<leaderHostname>-1.<subdomain>`), or populate `pod.Name` at admission / inject TPU variables after the leader name is known.

---

### 🔴 3. [High] Non-Idempotent `groupUniqueKey` Generation in `PodWebhook.Default` Causes Self-Anti-Affinity Deadlock on Webhook Reinvocation

**Location:**
- [`pkg/webhooks/pod_webhook.go`: `PodWebhook.Default` (L105–L160)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L105-L160)

#### Problem
If `PodWebhook.Default` is invoked more than once on a hash-mode leader pod (e.g., Kubernetes mutating webhook reinvocation when another mutating webhook modifies the pod), [L109](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L109) unconditionally generates a fresh random `groupUniqueKey` (`key2`) and overwrites `pod.Labels[GroupUniqueHashLabelKey]` and `pod.Labels[GroupIndexLabelKey]`:
```go
groupUniqueKey = genGroupUniqueKey(pod.Namespace, utilrand.String(16))
pod.Labels[leaderworkerset.GroupUniqueHashLabelKey] = groupUniqueKey
pod.Labels[leaderworkerset.GroupIndexLabelKey] = groupUniqueKey
```
However, all downstream mutations in `PodWebhook.Default` have idempotency guards that preserve values derived from the **first** invocation's `key1`:
- `pod.Spec.Hostname` ([L114](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L114): `if pod.Spec.Hostname == ""`) and `pod.Spec.Subdomain` (under `UniquePerReplica`) stay `<lwsName>-<key1[:8]>`.
- `LWS_LEADER_ADDRESS` on the leader (`addEnvVarsIfNotExists`) stays `<lwsName>-<key1[:8]>.<subdomain>.<namespace>`.
- [`SetExclusiveAffinities`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L247-L250) checks `if exclusiveAffinityApplied(*pod, topologyKey) { return }`, leaving the leader pod's `PodAffinity` requiring `group-key In [key1]` and `PodAntiAffinity` requiring `group-key Exists && group-key NotIn [key1]`, whereas the leader's own label and its worker pods' labels/affinities use `key2`.
- `SubGroupUniqueHashLabelKey` on the leader ([L147](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L147): `if ... pod.Labels[SubGroupIndexLabelKey] == ""`) remains `genGroupUniqueKey(key1, "0")`, whereas workers compute `genGroupUniqueKey(key2, "0")` ([L177](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L177)).

#### Impact
Under exclusive placement or subgroup exclusive placement, the leader pod's `PodAntiAffinity` (`group-key NotIn [key1]`) actively repels its own workers (`group-key = key2`), and its `PodAffinity` (`group-key In [key1]`) does not match its own label (`group-key = key2`), causing a permanent scheduling deadlock.

#### Recommended Fix
Only generate `groupUniqueKey` if `pod.Labels[leaderworkerset.GroupUniqueHashLabelKey] == ""`:
```go
groupUniqueKey = pod.Labels[leaderworkerset.GroupUniqueHashLabelKey]
if groupUniqueKey == "" {
	groupUniqueKey = genGroupUniqueKey(pod.Namespace, utilrand.String(16))
}
```

---

### 🔴 4. [High] Unvalidated 63-Character Limit Overflow on `controller-revision-hash` Labels and Hostnames in `LeaderWorkerSetWebhook` and `DisaggregatedSetWebhook`

**Locations:**
- [`pkg/webhooks/disaggregatedset/disaggregatedset_webhook.go`: `validateGeneratedNames` (L118–L166)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/disaggregatedset/disaggregatedset_webhook.go#L118-L166)
- [`pkg/webhooks/leaderworkerset_webhook.go`: `generalValidate` (L254–L261)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/leaderworkerset_webhook.go#L254-L261)
- [`pkg/webhooks/pod_webhook.go`: `hashLeaderHostname` (L238–L244)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L238-L244)

#### Problem
1. **28-character suffix on worker `controller-revision-hash` labels**:
   In `groupIdentity: Hash` mode:
   - Leader `Deployment` name: `<lwsName>`
   - Leader `ReplicaSet` name: `<lwsName>-<10-char-pod-template-hash>` (`len(lwsName) + 11`)
   - Leader `Pod` & worker `StatefulSet` name: `<lwsName>-<10-char-rs-hash>-<5-char-pod-suffix>` (`len(lwsName) + 17`)
   - Worker pod `controller-revision-hash` label value (assigned by the Kubernetes `StatefulSet` controller): `<workerStsName>-<10-char-sts-hash>` = **`len(lwsName) + 28`** characters.
   Because Kubernetes enforces a maximum length of 63 characters on label values (`controller-revision-hash`), **any hash-mode `LeaderWorkerSet` with `Size > 1` and `len(lws.Name) > 35` (`63 - 28`) fails to create worker pods** (`StatefulSet` controller fails pod creation with `metadata.labels: Invalid value: ... must be no more than 63 characters`).
2. **9-character suffix on leader `hostname` and `UniquePerReplica` `Service` name**:
   [`hashLeaderHostname`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/pod_webhook.go#L238-L244) formats `<lws.Name>-<8-char-hash>` (`len(lws.Name) + 9`) without truncating `lws.Name`. If `len(lws.Name) > 54`, `pod.Spec.Hostname` (and `pod.Spec.Subdomain` / headless `Service` name under `UniquePerReplica`) exceeds the 63-character DNS-1035 label limit and is rejected by the API server.
3. **`DisaggregatedSetWebhook.validateGeneratedNames` uses Ordinal math for Hash roles**:
   In [`validateGeneratedNames`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/webhooks/disaggregatedset/disaggregatedset_webhook.go#L140-L152), `maxSuffixLen` is computed as `1 + groupIndexDigits + 11` (typically `13` characters, assuming `<lwsName>-<ordinal>-<10charHash>`) even when `role.Spec.GroupIdentity == GroupIdentityHash` (where `maxSuffixLen` is `28`). Consequently, `DisaggregatedSetWebhook` admits generated LWS names of 36–50 characters that silently fail at runtime when creating worker pods.

#### Recommended Fix
- In `LeaderWorkerSetWebhook.ValidateGroupIdentity`, validate `len(lws.Name) <= 35` (when `Size > 1`, or truncate the worker `StatefulSet` name / name it after `leaderPod.Spec.Hostname` which only adds 9 chars instead of 17 chars!) and `len(lws.Name) <= 54` (when `Size == 1`).
- In `DisaggregatedSetWebhook.validateGeneratedNames`, use the hash-mode suffix length when `role.Spec.GroupIdentity == GroupIdentityHash`.

---

### 🔴 5. [High] `reconcileGroupReplacementGate` (`len(gated) - terminating`) Blocks Replacements During Scale-Down and Loses Surge Slots During Rollouts

**Locations:**
- [`pkg/controllers/pod_controller.go`: `reconcileGroupReplacementGate` (L749–L797)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L749-L797)
- [`pkg/controllers/pod_controller.go`: `countTearingDownGroups` (L802–L833)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L802-L833)

#### Problem
Under `groupReplacementPolicy: PostTermination` (the default), `reconcileGroupReplacementGate` admits a gated leader iff `rank < len(gated) - terminating`. Because this formula compares only the count of *currently gated* leaders (`len(gated)`) against *currently tearing-down* groups (`terminating`), without considering how many active (ungated, non-terminating) groups exist relative to desired capacity:
1. **Scale-down blocks replacement of remaining groups**:
   Suppose `replicas` is scaled down from `4` to `2`. Two groups (`G3`, `G4`) begin terminating (`terminating = 2`), and no replacement pods are created for them. If remaining group `G1` crashes while `G3` and `G4` are still terminating (`terminating = 3`), the `ReplicaSet` creates 1 replacement leader `L_rep1` (`len(gated) = 1`). Even after `G1` finishes terminating completely (`terminating = 2`: `G3` and `G4`), `len(gated) - terminating` is `1 - 2 = -1` (and even after `G3` finishes terminating, `1 - 1 = 0`). `L_rep1` remains stuck in `SchedulingGated` until **all** scaled-down groups (`G3` and `G4`) finish terminating, even though `G1` is completely gone and only 1 active group (`G2`) is running for `spec.replicas = 2`.
2. **Surge / scale-up slot is lost once ungated**:
   Suppose `replicas = 4, maxSurge = 1, maxUnavailable = 1` during a rolling update. The Deployment deletes 1 old leader (`terminating = 1`) and creates 2 new leaders (`L_new1, L_new2`, `len(gated) = 2`). `L_new1` (`rank = 0 < 2 - 1`) is admitted and leaves `gated`. Because the `ReplicaSet` deletes old leader pods in the background, the old leader pod disappears in ~1s while its workers continue terminating for ~25s (`terminating = 1`). If `L_new1` becomes Ready while the first old group's workers are still terminating, the Deployment sees 4 Ready groups (`maxUnavailable = 1` allows 3), deletes a second old leader (`terminating = 2`), and creates `L_new3` (`gated = [L_new2, L_new3]`, `len(gated) = 2`). Now `len(gated) - terminating = 2 - 2 = 0`: **both** `L_new2` and `L_new3` are held gated, losing the `maxSurge = 1` slot because `L_new1` is no longer in `gated`.
3. **Workqueue ordering emits spurious `GroupReplacementDeferred` events & `O(N^2)` event storm**:
   In [`SetupWithManager`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L1070-L1082), when a second gated leader `L2` is created while `L1` is gated (`G = 2, T = 1`), `podEventHandler` enqueues `L2` *before* `enqueueGatedLeaders` enqueues `L1`. `L2` (`rank = 1 >= 2 - 1`) is reconciled first and emits a misleading `GroupReplacementDeferred` event before `L1` (`rank = 0 < 1`) is reconciled and admitted. Furthermore, `enqueueGatedLeaders` re-enqueues all gated leaders on **every** pod deletion in the LWS (including each worker pod of a tearing-down group), emitting duplicate `GroupReplacementDeferred` events on every single pod deletion.

---

### 🔴 6. [High] Leaked `PodGroup` Resources on Group Replacement, Scale-Down, and Rollout

**Locations:**
- [`pkg/schedulerprovider/volcano_provider.go`: `ReconcileScheduling` (L53–L107) & `CreatePodGroupIfNotExists` (L183–L194)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/schedulerprovider/volcano_provider.go#L183-L194)
- [`pkg/schedulerprovider/kubernetes_provider.go`: `cleanupUnusedPodGroups` (L618–L683)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/schedulerprovider/kubernetes_provider.go#L618-L683)
- [`pkg/controllers/leaderworkerset_controller.go`: `SetupWithManager` (L364–L391)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_controller.go#L364-L391)

#### Problem
1. **`VolcanoProvider` (permanent leak when `spec.scheduling != nil`)**:
   When `lws.Spec.Scheduling != nil`, [`VolcanoProvider.CreatePodGroupIfNotExists`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/schedulerprovider/volcano_provider.go#L184-L187) sets `controllerOwner = lws` (`LeaderWorkerSet`) instead of `leaderPod` on each created `volcanov1beta1.PodGroup` (`<lwsName>-<40charHash>-<revision>`). Unlike `KubernetesProvider`, `VolcanoProvider` has **no `cleanupUnusedPodGroups` method**. In `groupIdentity: Hash` mode, every group replacement, scale-down, or rolling update draws a new 40-char `groupIndex` and creates a new `PodGroup` owned by `lws`, permanently leaking every superseded `volcanov1beta1.PodGroup` until the `LeaderWorkerSet` itself is deleted.
2. **`KubernetesProvider` (missed cleanup after background leader deletion)**:
   `cleanupUnusedPodGroups` only runs during `LeaderWorkerSetReconciler.reconcileHash` and keeps any `PodGroup` still referenced in `spec.schedulingGroup.podGroupName` by any existing Pod ([L660–L666](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/schedulerprovider/kubernetes_provider.go#L660-L666)). On scale-down or rolling update, the `ReplicaSet` deletes the leader pod in the background (~1s), which triggers GC of the worker `StatefulSet` in the background while worker pods remain terminating for another 20–30s. Both the leader `Pod` delete event and the worker `StatefulSet` delete event arrive while the worker pods still exist (`inUseGroups` is `true`), so `cleanupUnusedPodGroups` keeps the `PodGroup`. When the last worker pod finally finishes terminating, [`LeaderWorkerSetReconciler`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_controller.go#L371) ignores all worker pod events (`labels[WorkerIndexLabelKey] != "0"`), so `cleanupUnusedPodGroups` is not re-triggered.

---

### 🟡 7. [Medium] `updateStatusHash` Condition Bugs: `UpdateInProgress` Can Stick `True` (or Be Skipped) and `Degraded` Condition Is Never Set

**Locations:**
- [`pkg/controllers/leaderworkerset_hash.go`: `updateStatusHash` (L244–L262)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L244-L262)
- [`pkg/controllers/leaderworkerset_controller.go`: `setCondition` / `exclusiveConditionTypes` (L1172–L1226)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_controller.go#L1172-L1226)

#### Problem
1. **`UpdateInProgress` sticks `True` when transitioning to non-update `Progressing`**:
   [`exclusiveConditionTypes`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_controller.go#L1214-L1226) defines `(Available, Progressing)` and `(Available, UpdateInProgress)` as mutually exclusive, but `(Progressing, UpdateInProgress)` are **not** mutually exclusive. In `updateStatusHash`, once `UpdateInProgress` is set to `True` (`deploy.Status.UpdatedReplicas < deploy.Status.Replicas`), as soon as the old ReplicaSet scales down (`UpdatedReplicas == Replicas`) while new pods are still `SchedulingGated` or starting workers (`ReadyReplicas < lwsReplicas`), `updateStatusHash` enters the `else` branch ([L256](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_hash.go#L256)) and passes only `[Progressing=True]` to `setConditions`. Because `makeFalseCondition(LeaderWorkerSetUpdateInProgress, ...)` is not passed, `UpdateInProgress` stays `True` even if all pods are already on the new revision and a pod is merely unready or scaling up.
2. **`updateInProgress` is `false` during single-replica or final-step rollouts**:
   In Kubernetes `DeploymentStatus`, `UpdatedReplicas` counts all non-terminated pods of the new `ReplicaSet` (including `SchedulingGated` and unready pods). For a `replicas: 1` LWS (or on the final step of any rollout once the last old leader pod terminates), `deploy.Status.UpdatedReplicas == deploy.Status.Replicas` even while the new replacement group is still `SchedulingGated` and creating its worker `StatefulSet`, so `updateInProgress` (`UpdatedReplicas < Replicas`) evaluates to `false` while the rollout is still underway.
3. **Missing `Degraded` condition**:
   Unlike [`updateConditions`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/leaderworkerset_controller.go#L707) in `Ordinal` mode, `updateStatusHash` never sets `Degraded: False` (`"AsExpected"`) on `lws.Status.Conditions`.

---

### 🟡 8. [Medium] `PodReconciler.reconcilePod` Reads Mutable `Size` and `SubdomainPolicy` from Live `LeaderWorkerSet` Instead of Pod's `ControllerRevision`

**Location:**
- [`pkg/controllers/pod_controller.go`: `reconcilePod` (L205–L265)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L205-L265)

#### Problem
`reconcilePod` fetches the leader pod's `ControllerRevision` at [L266](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L266) and only applies it inside `constructWorkerStatefulSetApplyConfiguration` ([L982](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L982)). Earlier lines in `reconcilePod` inspect the **live** `leaderWorkerSet.Spec`:
- **`SubdomainPolicy` (`Shared` $\leftrightarrow$ `UniquePerReplica`)** ([L205](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L205)): If `SubdomainPolicy` is updated from `Shared` to `UniquePerReplica`, existing old-revision leader pods have `pod.Spec.Subdomain = lws.Name` (the shared headless Service controlled by `leaderWorkerSet`). When `reconcilePod` runs for an old-revision leader pod, it sees the live `SubdomainPolicy == UniquePerReplica` and calls `CreateHeadlessServiceIfNotExists` with `serviceName = lws.Name` and `owner = &pod`, which errors with `headless service <lwsName> has controller owner LeaderWorkerSet; expected <podName>`.
- **`Size` (`Size > 1` $\rightarrow$ `Size = 1`)** ([L246](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L246)): Old-revision hash leader pods carry the `leaderworkerset.sigs.k8s.io/group-ready` readiness gate (`Size > 1`). If `Size` is updated to `1` on the live LWS, `reconcilePod` returns early at [L247](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L247) for old-revision leader pods before calling `syncGroupReadyCondition`, leaving their `group-ready` readiness gate unsynchronized (`False`) and stalling rollouts when `maxUnavailable = 0`.

---

### 🟡 9. [Medium] `DisaggregatedSet` Does Not Sync `groupReplacementPolicy` During Rolling Updates

**Locations:**
- [`pkg/controllers/disaggregatedset/disaggregatedset_controller.go`: `reconcileSlice` (L394–L398) & `reconcileRoleSimple` (L500)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/disaggregatedset/disaggregatedset_controller.go#L394-L398)
- [`pkg/controllers/disaggregatedset/executor.go`: `ReconcileRollingUpdateNew` (L55–L85)](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/disaggregatedset/executor.go#L55-L85)

#### Problem
- [`LWSManager.SyncGroupReplacementPolicy`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/disaggregatedset/lws_manager.go#L162-L180) is only called inside `reconcileRoleSimple` (which only runs when `totalOldReplicas == 0`). Whenever a `DisaggregatedSet` rolling update is active (`len(oldRevisions) > 0 && totalOldReplicas > 0`), `reconcileSlice` dispatches to `executor.ReconcileRollingUpdateNew`, which never calls `SyncGroupReplacementPolicy` on either the new-revision or old-revision `LeaderWorkerSet`s. Switching a role's `groupReplacementPolicy` from `PostTermination` to `Immediate` mid-rollout (e.g. to unstick a blocked rollout) is ignored until the rolling update finishes.
- Additionally, because `DisaggregatedSet` creates a separate `LeaderWorkerSet` per revision (`<dsName>-<slice>-<revision>-<role>`), [`reconcileGroupReplacementGate`](file:///usr/local/google/home/ahg/go/src/kubernetes/kubernetes-sigs/lws/pkg/controllers/pod_controller.go#L753-L755) (which lists pods matching `leaderworkerset.sigs.k8s.io/name: lws.Name`) does not see terminating groups belonging to the old revision's `LeaderWorkerSet` during a `DisaggregatedSet` rolling update.
