# Rimio 设计文档（RFC 0010）

> 目标：不再让用户在 `registry.gossip` 中单独配置 `bind_addr/advertise_addr`；改为以 `initial_cluster.nodes[*].bind_addr/advertise_addr` 作为唯一地址真相源，并通过 internal 接口补齐 gossip 入群与状态同步所需信息。

## 背景

当前 `gossip` backend 需要单独配置：

- `registry.gossip.bind_addr`
- `registry.gossip.advertise_addr`
- `registry.gossip.seeds`

同时 `initial_cluster.nodes[]` 已经声明了节点 `bind_addr/advertise_addr`，导致地址配置存在重复来源，运维上容易出现不一致。

我们希望：

1. 简化配置（单一地址来源）；
2. 保留 `memberlist` 作为 gossip/membership 底座；
3. 让 `start/join` 仍保持清晰可控。

---

## 设计目标

1. `gossip` 地址不再由独立配置项维护；
2. `initial_cluster` 成为节点地址唯一真相源；
3. `join` 仅需 `REGISTRY_URL + --node`（可选校验参数保留）；
4. 不破坏现有 `redis://` / `etcd://` registry 路径；
5. 保持实现复杂度可控（早期项目优先简单落地）。

## 非目标

1. 不在本期实现完整动态扩容/重分片；
2. 不引入复杂多协议混部策略；
3. 不重写一套自研 memberlist 等价协议栈。

---

## 技术可行性评估

## 结论（TL;DR）

- **方案整体可行**，但有一个关键边界：
  - 若继续使用 `memberlist` crate，**不能**让 gossip 与 HTTP/internal 服务直接复用同一个监听 socket。
- 推荐落地方式：
  - 地址真相源来自 `initial_cluster.nodes[]`；
  - gossip 地址在运行时由节点地址**确定性派生**（无需用户单独配置）；
  - internal API 负责 bootstrap/gossip 元信息发现与反熵辅助。

## 关键原因

1. `memberlist` 需要独立传输层（TCP/UDP/stream layer），不能直接挂在 axum 的 HTTP 路由上；
2. `bind_addr` 当前用于业务 HTTP 监听，若同端口复用会产生协议冲突；
3. 因此“只用 node 地址 + 不让用户填 gossip 地址”是可行的，但实现上需**派生 gossip 地址**或单独传输端口约定。

## 可行方案对比

### A. 同端口复用（HTTP 与 memberlist 共用 `bind_addr`）

- 可行性：**低（不推荐）**
- 问题：监听冲突、协议复用复杂、实现侵入大。

### B. 地址派生（推荐）

- 可行性：**高（推荐）**
- 思路：
  - 主机部分来自 `initial_cluster.nodes[*].bind_addr/advertise_addr`；
  - gossip 端口通过确定性规则派生（例如 `service_port + offset`）；
  - 用户不再配置 gossip bind/advertise。

### C. 纯 internal API gossip（不使用 memberlist 传输）

- 可行性：**中（本期不推荐）**
- 问题：会演变为自研 gossip/failure detector，偏离“复用 memberlist”目标。

---

## 提案（推荐方案 B）

## 1) 配置模型调整

### 现状（简化）

```yaml
registry:
  backend: gossip
  gossip:
    bind_addr: 0.0.0.0:8400
    advertise_addr: 10.0.0.1:8400
    seeds: [10.0.0.2:8400]
```

### 目标

- 删除（或弃用）用户侧：`registry.gossip.bind_addr` / `registry.gossip.advertise_addr`；
- 节点地址统一从 `initial_cluster.nodes[]` 获取；
- `registry.gossip` 仅保留可选“全局策略参数”（如 `port_offset`、`fanout`、`timeouts`）。

示意：

```yaml
registry:
  backend: gossip
  namespace: local-cluster-001
  gossip:
    # 可选，默认 10000
    port_offset: 10000

initial_cluster:
  nodes:
    - node_id: node-1
      bind_addr: 127.0.0.1:19080
      advertise_addr: 127.0.0.1:19080
```

## 2) gossip 地址派生规则

给定节点 `service_bind_addr` 与 `service_advertise_addr`：

- `gossip_bind_addr = host(service_bind_addr):(port(service_bind_addr) + port_offset)`
- `gossip_advertise_addr = host(service_advertise_addr):(port(service_advertise_addr) + port_offset)`

约束：

1. 派生端口必须在 `u16` 范围；
2. 派生地址冲突则启动失败并给出明确错误；
3. `join --listen/--advertise-addr` 仅用于 service 地址一致性校验，不再直接作为 gossip bind/advertise 输入。

## 3) internal API（用于 gossip 辅助信息）

新增只读 internal 接口（最小集）：

1. `GET /internal/v1/cluster/bootstrap`
   - 返回 bootstrap state（与 registry 版本一致）
2. `GET /internal/v1/cluster/gossip-seeds`
   - 返回当前节点视角下可用 gossip seed 地址列表（已派生后的地址）

用途：

- `join cluster://...` 可先打任一可达 seed 的 internal API，获取 bootstrap 与 seeds；
- 降低命令行 seed 维护成本；
- 为后续反熵诊断提供统一观测入口。

## 4) start/join 行为

### `start --conf ... --node ...`

1. 从 `config.yaml` 获取 `initial_cluster`；
2. 根据当前 `node_id` 计算本地 gossip 地址；
3. 若 registry 已有 bootstrap -> 走 join 语义；否则建群。

### `join REGISTRY_URL --node ...`

1. `cluster://` 视为 service seed 地址；
2. 从 seed internal API 拉 bootstrap + gossip-seeds；
3. 校验 `--node` 存在且参数一致；
4. 按派生地址启动 memberlist 并 join。

---

## 兼容性与迁移

1. 保留一小段兼容窗口：
   - 若配置里仍有 `registry.gossip.bind_addr/advertise_addr`，记录 warning 并忽略；
2. 文档与示例统一迁移到“node 地址单来源”；
3. `redis://`、`etcd://` join 路径保持不变。

---

## 对代码的影响点

1. `rimio-server/src/config.rs`
   - Gossip 配置结构裁剪（去掉 bind/advertise）；
   - 新增 `port_offset`（可选）。
2. `rimio-server/src/main.rs`
   - `start/join` 地址派生逻辑；
   - `cluster://` join 的 seed/internal-bootstrap 流程。
3. `rimio-server/src/server/mod.rs` + internal handlers
   - 增加 `bootstrap` 与 `gossip-seeds` internal 只读接口。
4. `rimio-core/src/registry/factory.rs`
   - gossip builder 输入从“显式地址配置”改为“派生后地址”。

---

## 风险与缓解

1. **风险：派生端口与现有端口冲突**
   - 缓解：启动前端口探测 + 明确报错 + 可配置 `port_offset`。
2. **风险：join 早期 seed 不可达**
   - 缓解：`cluster://` 多 seed，任一可达即可。
3. **风险：地址变更导致历史节点入群失败**
   - 缓解：bootstrap 版本化 + 变更需全量重启窗口。

---

## 验收标准

1. 用户配置中不再需要 `registry.gossip.bind_addr/advertise_addr`；
2. `start/join` 均可从 `initial_cluster` 地址派生 gossip 通信地址并成功运行；
3. `join cluster://...` 可经 internal API 获得 bootstrap + gossip seeds；
4. `redis://` 与 `etcd://` join 路径行为不回归；
5. 相关集成测试覆盖地址派生、seed 发现、冲突报错路径。

---

## 开放问题

1. `port_offset` 是否固定默认 + 禁止配置，还是允许环境差异化配置？
2. internal bootstrap API 是否需要鉴权（当前默认内网信任）？
3. 是否需要提供 `rimio adm derive-gossip-addrs` 诊断命令？
