#ifndef STABLE_RNG_H
#define STABLE_RNG_H

#include "r_params.h"

int stable_rand_10pct(int table, int scale, ds_key_t index);
int get_current_table_id(void);

#endif /* STABLE_RNG_H */