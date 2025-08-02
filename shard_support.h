#ifndef SHARD_SUPPORT_H
#define SHARD_SUPPORT_H

#include "tables.h"

/* Tables supporting shard filtering */
static const int SHARD_SUPPORTED_TABLES[] = {
    ITEM,
    INVENTORY,
    CATALOG_SALES,
    CATALOG_RETURNS,
    STORE_SALES,
    STORE_RETURNS,
    WEB_SALES,
    WEB_RETURNS
};

#define NUM_SHARD_SUPPORTED_TABLES (sizeof(SHARD_SUPPORTED_TABLES) / sizeof(SHARD_SUPPORTED_TABLES[0]))

/* Check if table supports sharding */
#define TABLE_SUPPORTS_SHARDING(table_id) \
    ((table_id) == ITEM || \
     (table_id) == INVENTORY || \
     (table_id) == CATALOG_SALES || \
     (table_id) == CATALOG_RETURNS || \
     (table_id) == STORE_SALES || \
     (table_id) == STORE_RETURNS || \
     (table_id) == WEB_SALES || \
     (table_id) == WEB_RETURNS)

/* Get shard-supported table names as string */
#define SHARD_SUPPORTED_TABLES_STR "item, inventory, catalog_sales, catalog_returns, store_sales, store_returns, web_sales, web_returns"

#endif /* SHARD_SUPPORT_H */
