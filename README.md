# 🏦 Banks ETL Project

A complete ETL (Extract, Transform, Load) pipeline that scrapes real-world banking data, transforms it using exchange rates, and stores it in both CSV and SQLite database formats — built as part of the IBM Data Engineering Professional Certificate.

---

## 📌 Project Description

This project was developed to simulate a real-world scenario where a multinational firm requests a quarterly report of the top 10 largest banks in the world by market capitalization (USD), with values converted into GBP, EUR, and INR.

The pipeline includes:

- 🔍 **Data Extraction** using `requests` and `BeautifulSoup` from Wikipedia  
- 🔄 **Transformation** using `pandas` and `numpy` to convert currencies  
- 💾 **Loading** to both `.csv` and an SQLite database (`Banks.db`)  
- 🧪 **Querying** the database for business use cases  
- 🧠 **Logging** using `datetime` to track all progress  
- ✅ Basic unit testing  



