import sys
# caution: path[0] is reserved for script path (or '' in REPL)
sys.path.insert(1, 'generateData\\helpers')

import helpers.makedirectory
import helpers.generate_master_product
import helpers.generate_customers
import helpers.generate_tiktok_marketing
import helpers.generate_pos_elements
import helpers.generate_src_products
import helpers.generate_orders
import helpers.generate_pos_invmov

print("Data generation completed.")