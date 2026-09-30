# Data

This project uses the Olist Brazilian E-Commerce dataset. The CSV files are not stored in this repository due to their size, so they need to be downloaded before running `assessment2.ipynb`.

| | |
|---|---|
| **Dataset** | Olist Brazilian E-Commerce Public Dataset |
| **Source** | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |
| **Licence** | CC BY-NC-SA 4.0 |
| **Size (uncompressed)** | Approximately 125 MB across nine CSV files |

## Extracting the data

### Kaggle account

Kaggle only allows downloads from signed-in users. New user accounts can be created as follows:

1. Visit https://www.kaggle.com and click **Register**.
2. Follow the prompts to complete the registration.
3. Verify the account using the link in Kaggle's confirmation email.
4. Sign in.

### Downloading and placing the files

1. Once signed in, the dataset page should be accessed using the link above.
2. Click **Download**, then **Download dataset as zip**.
3. Once downloaded, extract the zip file into the `data/raw/` folder of this repository.

We note that our notebook reads each file from `data/raw/` using its original Kaggle filename, so it is important that the files should not be renamed or placed in a subfolder.

## Checking the files

From the repository root, run:

```bash
ls -1 data/raw/*.csv | wc -l
```

When this is run, it should print `9` since there are 9 files within the dataset.

| File | Rows | Columns | Used in this project |
|---|---:|---:|:---:|
| `olist_geolocation_dataset.csv` | 1,000,163 | 5 | Yes |
| `olist_order_items_dataset.csv` | 112,650 | 7 | Yes |
| `olist_order_payments_dataset.csv` | 103,886 | 5 | No |
| `olist_orders_dataset.csv` | 99,441 | 8 | Yes |
| `olist_customers_dataset.csv` | 99,441 | 5 | Yes |
| `olist_order_reviews_dataset.csv` | 88,224 | 7 | No |
| `olist_products_dataset.csv` | 32,951 | 9 | Yes |
| `olist_sellers_dataset.csv` | 3,095 | 4 | Yes |
| `product_category_name_translation.csv` | 71 | 2 | Yes |

As we can see above, there are two excluded dataset. THe reasons for excluding them will be explored in detail within  Part A of the `assessment2.ipynb` notebook.

## Generated data

Running Part A of the `assessment2.ipynb` notebook creates `data/stream_data.parquet/`. This holds the 30% of orders set aside from model training, which `producer.py` will later need to send through Kafka in order to simulate a real-time stream. Since the split uses a fixed random seed (42), rerunning Part A will always giv us the exact same subset. As such, this folder is not committed to our repo.