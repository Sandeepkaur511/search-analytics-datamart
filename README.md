# Search Analytics DataMart

**E-commerce Search Performance Pipeline**

Built a centralized, pre-aggregated datamart that serves as the **single source of truth** for all search KPIs (Pageviews, CTR, PDP clicks, Leads, Contacts) across **Mobile, Desktop, Android & iOS**.

### Key Achievements
- Reduced DWH load by migrating from repeated non-materialized views to a daily materialized table
- Full segment support for Power BI (platform, country, city tier, keyword type, price filters, etc.)
- Retained grain-level `user_id` in Daily mart for accurate unique user counting
- Automated daily/weekly/monthly refresh with proper retention (42 days / 52 weeks / 12 months)

### Technical Highlights
- Complex URL & referrer parsing using regex
- Multi-source data integration (web logs + backend lead/call tables)
- Different aggregation logic for Daily vs Weekly/Monthly
- Incremental loading + historical cleanup
- Designed for interactive Power BI slicing

### Tech Stack
- **Database**: PostgreSQL / Amazon Redshift
- **Language**: PL/pgSQL Stored Procedure
- **Tools**: Staging tables, regex parsing, conditional aggregation

### Project Structure
- `sql/sp_search_analytics.sql` → Main stored procedure (Daily/Weekly/Monthly)
- `docs/BRD.md` → Business Requirement Document (clean version)

---

**Status**: Production Ready (used in real analytics dashboards)
