# Step การทำงานมีดังนี้
## 1.Data source 
นำ Raw data วางไว้ที่ s3 เพื่อเป็น data source จำลองในการดึงข้อมูลมาใช้งาน
สร้าง Bucket ใน s3 

ทำการสร้าง Folder landing เก็บ Raw_data 


## 2.    Datalake Zone 
เป็นส่วนที่ใช้จัดเก็บไฟล์ต่างๆ ที่ได้ทำการดึงข้อมูลมา และนำมาจัดเก็บไว้ ก่อนที่จะนำไป ETL หรือ Transform ใน Step ถัดไป โดยจะทำการสร้างไฟล์ datalake_s3.py เพื่อดึงข้อมูลจาก Folder ในส่วนของ Landing Zone มาจัดเก็บ ในส่วนนี้จะเก็บไว้ใน s3 โดยใช้คำสั่ง Spark ให้การดึงข้อมูลและสร้างตารางในการจัดเก็บ 
ทำการสร้าง Folder Export เก็บ clean data
ตั้งค่าการเชื่อมต่อ AWS กับ ไฟล์ py
```sh
ใช้คำสั่ง $ cat ~/.aws/credentials 
```
aws_access_key_id
aws_secret_access_key
aws_session_token



ทำการรันผ่าน docker compose และเข้าไปรันไฟล์ที่ port 8888
```sh
cd capstone-project
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
docker-compose up
port 8888
```
สร้างไฟล์ datalake_s3.py บน Jupyter 


## 3. Data Warehouse zone 
     เป็นส่วนที่ใช้จัดเก็บไฟล์ที่ผ่านการ Transform หรือ ETL ให้อยู่ในโครงสร้างที่ต้องการและสามารถนำไปใช้ต่อได้อย่างสะดวก โดยเก็บไว้ที่ Amazon Redshift  และใช้ Airflow control ทำหน้าที่เป็น Data Pipeline ควบคุมการไหลของข้อมูล และสามารถ Monitor การทำงานของ Workflow ได้
ทำการ Create Cluster ใน Amazon Redshift

ทำการสร้างไฟล์ datawarehouse.py เก็บไว้ใน Folder Dags และทำการสร้างตารางที่ต้องการนำไปใช้งานตามที่ออกแบบไว้  (create_tables[สร้างตาราง] >> truncate_tables[ลบตารางที่เคยสร้าง] >> load_staging_tables >> insert_tables) 


ทำการ docker compose และเข้าไปที่ port 8080
```sh
cd capstone-project
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up
port 8080
```
กดรัน task บน Airflow

ทดสอบ query ข้อมูล บน Amazon Redshift และ Download เป็น csv


## 4. Data Visualization
เมื่อได้ข้อมูลที่พร้อมนำมาใช้งานตามโครงสร้างที่ได้ออกแบบแล้ว ได้ทำงานการนำข้อมูลดังกล่าวไปใช้งานต่อ ในส่วนของ Data Visualization เพื่อให้สามารถนำไปทำกราฟที่ทำให้เข้าใจข้อมูลได้ง่ายขึ้น โดยทำการสร้าง Dashboard จาก Power Bi มีการนำเข้าไฟล์ csv ที่ได้จาก Amazon Redshift

