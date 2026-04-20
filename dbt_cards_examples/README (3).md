# dot.cards dbt Examples

These models are **not part of the crypto pipeline above.**

They demonstrate how I would architect dot.cards' actual dbt layer — staging models for Segment, Shopify, and Stripe, and mart models answering dot.cards' core business questions around revenue, retention, and user engagement.

The crypto pipeline in this repo uses the same architecture patterns — raw → staging → marts → tests. Swap the APIs and source tables. The patterns are identical.

## Staging Layer
| Model | Source | Purpose |
|-------|--------|---------|
| `stg_segment__events.sql` | Segment CDP | Identity resolution, event standardization, JSON extraction, deduplication |
| `stg_shopify__orders.sql` | Shopify | Orders, line items, customers, delivery status |
| `stg_stripe__charges.sql` | Stripe | Payments, subscriptions, refunds, revenue recognition |

## Mart Layer
| Model | Purpose |
|-------|---------|
| `fct_revenue_by_channel.sql` | Unified revenue across e-commerce, SaaS (dot.teams), and mobile |
| `fct_user_retention.sql` | Cohort retention by acquisition channel and card type |

## Architecture Philosophy
- **Staging:** One model per source. Clean, type, rename, document. No business logic.
- **Marts:** Business logic lives here. Cross-source joins, enrichment, decision-ready metrics.
- **Tests:** Every model has automated dbt tests. Data quality is not optional.
- **Documentation:** Every column is described. Self-serve analytics requires trust.

Built by Proud Kudzai Ndlovu — Analytics Engineer | dbt · BigQuery · SQL · Python
