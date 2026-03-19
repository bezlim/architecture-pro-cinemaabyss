# CinemaAbyss Target Architecture
## C4 Container Diagram

**Диаграмма:** [target-architecture.puml](./target-architecture.puml)

---

## C4 Уровень: Контейнеры

| Уровень | Компоненты |
|---------|------------|
| **API Layer** | API Gateway, Mobile BFF, Web BFF, TV BFF |
| **Domain Services** | Users, Movies, Payments, Subscriptions, Favorites |
| **Event Bus** | Apache Kafka |
| **Internal Services** | Notification, Analytics, ML |
| **Observability** | Elastic APM (ELK), Grafana |

---

## Data Flow

```
┌──────────────┐
│  Пользователь │
└──────┬───────┘
       │ HTTPS
       ▼
┌──────────────┐
│ API Gateway  │
└──────┬───────┘
       │
       ├──→ Mobile BFF
       ├──→ Web BFF
       └──→ TV BFF
              │
              ▼
       ┌──────────────────┐
       │ Domain Services  │
       │ Users, Movies,   │
       │ Payments, etc.   │
       └────────┬─────────┘
                │
                ├──→ PostgreSQL (CRUD)
                │
                └──→ Kafka (Events)
                        │
                        ├──→ Notification
                        ├──→ Analytics
                        └──→ ML
```

---

## Технологический стек

| Компонент | Технология |
|-----------|------------|
| Backend | Go 1.21+ |
| BFF | Go |
| API Gateway | Go / Kong |
| Databases | PostgreSQL 14+ |
| Event Bus | Kafka 3.x |
| Analytics | ClickHouse |
| ML | Python |
| Observability | ELK (Elastic APM) + Grafana |
| Kubernetes | K8s 1.19+ |
| Service Mesh | Istio |
| CI/CD | GitHub Actions |

---

## Ссылки

- [Диаграмма](./target-architecture.puml)
- [API Specification](../api-specification.yaml)
