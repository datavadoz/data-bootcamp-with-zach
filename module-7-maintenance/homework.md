**DE Team**
- DE-001: Specializes in profit attribution modeling
- DE-002: Specializes in engagement attribution modeling
- DE-003: Specializes in growth attribution modeling
- DE-004: Has experiences both profit and growth attribution modeling

# Primary and secondary owner of following pipelines

| Pipeline | Primary Owner | Secondary Owner |
| -------- | ------- | ------- |
| Profit ETL (base profit model) | DE-001 | DE-002 |
| Engagement ETL (base engagement model) | DE-002  | DE-003 |
| Growth ETL (base growth model) | DE-003 | DE-004 |
| Unit-level profit for experiments | DE-004 | DE-002 |
| Daily growth for experiments | DE-004 | DE-003 |

# The on-call strategy
- Primary and secondary on-call each week
- Shift boundaries: Mon 09:00 to Mon 09:00 UTC+7 timezone
- Holiday handling: Extra leaves in lieu for recovery on the next week
- Escalation path beyond the secondary: DE manager
- Call out alerting system and communication channels: Opsgenie + Slack

| Week | Primary Owner | Secondary Owner |
| -------- | ------- | ------- |
| 1 | DE-001 | DE-002 |
| 2 | DE-002 | DE-003 |
| 3 | DE-003 | DE-004 |
| 4 | DE-004 | DE-001 |
| 5 | DE-001 | DE-002 |
| 6 | DE-002 | DE-003 |
| 7 | DE-003 | DE-004 |
| 8 | DE-004 | DE-001 |

**Note**: If primary is on holiday, the next secondary steps up and gets credit + a comp week in two cycles

# Creating run books for all pipelines that report metrics to investors

**Profit ETL**:
- Primary owner: DE-001
- Secondary owner: DE-002
- Upstream owner: Software Engineer Team
- Downstream owner: Investor Dashboard
- Purpose of the metric: A day by day profit report to investor
- SLA: Data must be refreshed at 5AM daily
- Common issues:
  - The pipeline gets stuck and data is not ready in time.
  - Data is inaccurate because of upstream delay (master data).

**Aggregate growth reported to investors**:
- Primary owner: DE-003
- Secondary owner: DE-004
- Upstream owner: Software Engineer Team
- Downstream owner: Investor Dashboard
- Purpose of the metric: A day by day growth report to investor
- SLA: Data must be refreshed at 5AM daily
- Common issues:
  - The pipeline gets stuck and data is not ready in time.
  - Data is inaccurate because of upstream delay (master data).

**Aggregate engagement reported to investors**:
- Primary owner: DE-002
- Secondary owner: DE-003
- Upstream owner: Software Engineer Team
- Downstream owner: Investor Dashboard
- Purpose of the metric: A day by day engagement report to investor
- SLA: Data must be refreshed at 5AM daily
- Common issues:
  - The pipeline gets stuck and data is not ready in time.
  - Data is inaccurate because of upstream delay (master data).
