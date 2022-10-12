# Building a Data Lake
## 1. ออกแบบ Data Modeling ตามที่ต้องการ
## 2. ทำการ เข้าไปที่ Folder 04 ด้วยคำสั่ง 
```sh
cd 04-building-a-data-lake 
```
และกำหนดให้สามารถเขียนไฟล์ได้ ด้วยคำสั่ง 
```sh
sudo chmod 777 . 
```
หลังจากนั้นใช้คำสั่ง เพื่อรัน Docker
```sh
docker-compose up 
```
## 3. เข้าไปที่ tab port เลือก port 8888 และนำ token ไปกรอกในช่อง
## 4. จากนั้นเข้าไปที่ไฟล์ etl_local_0043.ipynb เพื่อทำการสร้างตารางตามที่ออกแบบโดยใช้คำสั่ง spark และ sql จะได้ Folder ดังนี้

#### actors : [actors](https://github.com/Plengnattakan/swu-ds525/tree/main/04-building-a-data-lake/actor)

#### repo : [repo](https://github.com/Plengnattakan/swu-ds525/tree/main/04-building-a-data-lake/repo)

#### events : [events](https://github.com/Plengnattakan/swu-ds525/tree/main/04-building-a-data-lake/events)
## 5. เมื่อเสร็จ ให้ทำการ Down Docker ด้วยคำสั่งนี้ 
```sh
docker-compose down 
```