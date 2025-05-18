# Airbnb_Data-Pipeline

Proyek ini dibuat sebagai bagian dari pembelajaran di bootcamp data analytics, dengan fokus membangun data pipeline end-to-end menggunakan PySpark. Pipeline ini memproses dataset Airbnb dan terdiri dari beberapa komponen utama:

- ETL (Extract, Transform, Load): Data mentah dari Airbnb di-extract, lalu dibersihkan dan ditransformasi menggunakan PySpark agar konsisten dan siap digunakan. Setelah itu, data dimuat untuk disimpan.

- Validasi Data dengan Great Expectations (GX): Untuk menjaga kualitas data, pipeline ini dilengkapi dengan validasi menggunakan Great Expectations, seperti pengecekan skema, nilai null, dan tipe data.

- Pemuatan Data ke MongoDB: Setelah melalui proses validasi, data bersih dimuat ke MongoDB, sebuah database NoSQL, yang dapat digunakan untuk analisis lebih lanjut atau pengembangan aplikasi.

Pipeline ini dirancang agar dapat diskalakan dan diotomatisasi, sehingga mampu memperbarui data Airbnb secara efisien dan andal untuk berbagai kebutuhan ke depan.
