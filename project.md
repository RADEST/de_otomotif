**Data Engineering Take-Home Challenge: Business Case**

Perusahaan retail otomotif "Maju Jaya" sedang membangun pusat data dan migrasi data dari yang tadinya banyak menggunakan excel. Anggap datawarehouse yang digunakan adalah MySQL.

Di MySQL, sudah ada table customers_raw, sales_raw, dan after_sales_raw.

Masih ada source data dari filesharing yaitu customer*addresses\_\_yyyymmdd*.csv.

**customers_raw**

| id  | name          | dob        | created_at              |
| --- | ------------- | ---------- | ----------------------- |
| 1   | Antonio       | 1998-08-04 | 2025-03-01 14:24:40.012 |
| 2   | Brandon       | 2001-04-21 | 2025-03-02 08:12:54.003 |
| 3   | Charlie       | 1980/11/15 | 2025-03-02 11:20:02.391 |
| 4   | Dominikus     | 14/01/1995 | 2025-03-03 09:50:41.852 |
| 5   | Erik          | 1900-01-01 | 2025-03-03 17:22:03.198 |
| 6   | PT Black Bird | NULL       | 2025-03-04 12:52:16.122 |

**sales_raw**

| vin        | customer_id | model  | invoice_date | price       | created_at              |
| ---------- | ----------- | ------ | ------------ | ----------- | ----------------------- |
| JIS8135SAD | 1           | RAIZA  | 2025-03-01   | 350.000.000 | 2025-03-01 14:24:40.012 |
| MAS8160POE | 3           | RANGGO | 2025-05-19   | 430.000.000 | 2025-05-19 14:29:21.003 |
| JLK1368KDE | 4           | INNAVO | 2025-05-22   | 600.000.000 | 2025-05-22 16:10:28.12  |
| JLK1869KDF | 6           | VELOS  | 2025-08-02   | 390.000.000 | 2025-08-02 14:04:31.021 |
| JLK1962KOP | 6           | VELOS  | 2025-08-02   | 390.000.000 | 2025-08-02 15:21:04.201 |

**after_sales_raw**

| service_ticket | vin        | customer_id | model  | service_date | service_type | created_at              |
| -------------- | ---------- | ----------- | ------ | ------------ | ------------ | ----------------------- |
| T124-kgu1      | MAS8160POE | 3           | RANGGO | 2025-07-11   | BP           | 2025-07-11 09:24:40.012 |
| T560-jga1      | JLK1368KDE | 4           | INNAVO | 2025-08-04   | PM           | 2025-08-04 10:12:54.003 |
| T521-oai8      | POI1059IIK | 5           | RAIZA  | 2026-09-10   | GR           | 2026-09-10 12:45:02.391 |

**customer_addresses (csv)**

| id  | customer_id | address                          | city              | province    | created_at              |
| --- | ----------- | -------------------------------- | ----------------- | ----------- | ----------------------- |
| 1   | 1           | Jalan Mawar V, RT 1/RW 2         | Bekasi            | Jawa Barat  | 2026-03-01 14:24:40.012 |
| 2   | 3           | Jl Ababil Indah                  | Tangerang Selatan | Jawa Barat  | 2026-03-01 14:24:40.012 |
| 3   | 4           | Jl. Kemang Raya 1 No 3           | JAKARTA PUSAT     | DKI JAKARTA | 2026-03-01 14:24:40.012 |
| 4   | 6           | Astra Tower Jalan Yos Sudarso 12 | Jakarta Utara     | DKI Jakarta | 2026-03-01 14:24:40.012 |

_Data hanya sample untuk mendapat gambaran kontennya_

**_Task 1 / Datalanding_**

Buatlah pipeline baru untuk ingest data dari file customer_address_yyyymmdd.csv tersebut untuk dimasukkan ke dalam MySQL. File csv akan keluar harian di folder, tugasmu membuat code python untuk consume file harian.

**_Task 2 / Datamart_**

a. Beberapa table existing mungkin perlu diclean. Buat script python dengan library apapun atau SQL, untuk membersihkan apa yang kamu rasa perlu diclean

b. User meminta report seperti berikut:

| periode        | class                                                                         | model | total                    |
| -------------- | ----------------------------------------------------------------------------- | ----- | ------------------------ |
| format YYYY-MM | LOW: range 100-250juta<br><br>MEDIUM: range 250-400juta<br><br>HIGH: >400juta |       | jumlah nominal penjualan |

| periode     | vin | customer_name | address | count_service                       | priority                                                          |
| ----------- | --- | ------------- | ------- | ----------------------------------- | ----------------------------------------------------------------- |
| format YYYY |     |               |         | jumlah berapa kali melakukan servis | HIGH: servis >10x<br><br>MED: servis 5-10x<br><br>LOW: servis <5x |

Buatkan query untuk 2 table tersebut, dengan asumsi job akan berjalan daily.

**_Task 3 / Design_**

Dari semua table yang sudah kamu ketahui, buatlah desain rancangan akan dibuat seperti apa datawarehousenya, dari level 1/raw sampai level datamart. Outputnya adalah gambar rancangan db. Bebas gunakan tools apapun, draw.io, visio, paint, dan lainnya.

**_Task 4 / Point plus_**

Buat semuanya tadi dalam _docker-compose environment_ dan cantumkan link github disini, kamu bebas membuat data dummy sendiri atau menggunakan sample di atas.

**_Answer_**