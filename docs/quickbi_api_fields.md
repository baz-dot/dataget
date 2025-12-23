# Quick BI API 字段映射

## 字段对照表

| 参数名 (API返回) | 绑定字段 | 描述 |
|-----------------|---------|------|
| stat_date | stat_date(day) | 日期 |
| campaign_id | campaign_id | 广告系列 ID |
| campaign_name | campaign_name | 广告系列名称 |
| status | status | 投放状态 (Active/Stopped) |
| drama_id | drama_id | 剧集 ID |
| drama_name | drama_name | 剧集名称 |
| genre_names | genre_names | 题材分类 |
| channel | channel | 渠道 (如 google) |
| country | country | 国家/地区 |
| device | device | 设备类型 |
| language | language | 语言 |
| optimizer | optimizer | 优化师 |
| spend | spend | spend($) |
| impressions | impressions | impressions |
| clicks | clicks | clicks |
| lp_pv | lp_pv | PV |
| lp_clicks | lp_clicks | Button Clicks |
| new_users | new_users | New users_MKT |
| new_payers | new_payers | Paid users |
| new_ad_revenue | new_ad_revenue | Ad Revenue($) |
| new_sub_revenue | new_sub_revenue | Subscription Revenue($) |
| new_user_revenue | new_user_revenue | New users_MKT Revenue($) |
| new_iap_purchases | new_iap_purchases | Purchase |
| new_sub_purchases | new_sub_purchases | Subscription |

## 计算字段

| 字段名 | 计算公式 | 描述 |
|-------|---------|------|
| cpi | spend / new_users | Cost Per Install |
| cac | spend / new_users | Customer Acquisition Cost |
| ctr | clicks / impressions | Click Through Rate |
| cvr | new_users / clicks | Conversion Rate |
| cpc | spend / clicks | Cost Per Click |
| cpm | (spend / impressions) * 1000 | Cost Per Mille |
| d0_roas | new_user_revenue / spend | D0 ROAS |
| media_d0_roas | new_user_revenue / spend | Media D0 ROAS |
| new_user_paying_rate | new_payers / new_users | New User Paying Rate |
| arpu | new_user_revenue / new_users | Average Revenue Per User |
