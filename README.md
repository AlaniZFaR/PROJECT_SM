# Proyecto ETL en Azure Databricks – Arquitectura Medallion

## 📌 Descripción general
Este proyecto implementa un **proceso ETL (Extract, Transform, Load)** en **Azure Databricks**, utilizando la **arquitectura Medallion (Bronze, Silver, Gold)** para el procesamiento y análisis de datos.  
Se trabaja con cuatro datasets principales relacionados a pedidos y productos, y se manejan **dos entornos independientes: Desarrollo y Producción**, garantizando control, calidad y escalabilidad del pipeline.

---

## 📂 Datasets utilizados
El proyecto consume los siguientes archivos de origen:

- **aisles**: información de pasillos o categorías secundarias de productos.
- **departments**: departamentos o categorías principales.
- **products**: catálogo de productos, asociados a aisles y departments.
- **orders**: información de pedidos realizados por los clientes.

Estos datasets representan una estructura típica de datos transaccionales y maestros.

---

## 🏗️ Arquitectura Medallion
El procesamiento de datos sigue el enfoque Medallion:

### 🥉 Bronze (Raw)
- Ingesta directa de los archivos originales.
- Datos almacenados sin transformaciones.
- Preserva el estado original para auditoría y trazabilidad.

### 🥈 Silver (Clean & Conformed)
- Limpieza de datos (nulos, formatos, duplicados).
- Normalización de columnas.
- Enriquecimiento mediante joins entre datasets (ej. products con aisles y departments).
- Datos listos para análisis.

### 🥇 Gold (Analytics)
- Datos agregados y modelados para análisis.
- Tablas optimizadas para consumo analítico y dashboards.
- Métricas listas para explotación por BI o ciencia de datos.

---

## ⚙️ Proceso ETL
1. **Extract**
   - Lectura de archivos fuente (CSV).
   - Carga inicial en capa Bronze.

2. **Transform**
   - Limpieza y validación en Silver.
   - Relacionamiento entre pedidos, productos y categorías.

3. **Load**
   - Persistencia de tablas finales en Gold.
   - Optimización para consultas analíticas.

---

## 🌱 Entornos del proyecto

### 🧪 Desarrollo (DEV)
- Usado para pruebas y validaciones.
- Cambios frecuentes en lógica y transformaciones.
- Permite experimentar sin afectar datos productivos.

### 🚀 Producción (PROD)
- Procesos estables y validados.
- Datos confiables para consumo final.
- Control de versiones y ejecución controlada.

Cada entorno maneja sus propias rutas, esquemas y configuraciones dentro de Databricks.

---

## 🛠️ Tecnologías utilizadas
- **Azure Databricks**
- **Apache Spark**
- **Delta Lake**
- **Arquitectura Medallion**
- **Python / PySpark**
- **Azure Data Lake Storage (ADLS)**

---

## 📊 Casos de uso
- Análisis de pedidos y comportamiento de compra.
- Segmentación de productos por departamentos y pasillos.
- Preparación de datos para dashboards en Power BI u otras herramientas BI.
- Base para modelos analíticos o de machine learning.

