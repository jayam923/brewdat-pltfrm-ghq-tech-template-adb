# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.4.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_system", "vertica", "2 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"source_system: {source_system}")

dbutils.widgets.text("target_zone", "ghq", "3 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "4 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "gld_ghq_tech_vertica", "5 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "vertica", "6 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

dbutils.widgets.text("data_interval_start", "2022-05-21T00:00:00Z", "7 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

dbutils.widgets.text("data_interval_end", "2022-05-22T00:00:00Z", "8 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"data_interval_end: {data_interval_end}")


# COMMAND ----------

import re
import sys

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils

# Print a module's help
help(common_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

try:
    df_consum_1 = spark.sql(
    """
    SELECT DISTINCT 
    (case when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) 
then 'Directas' 
    when Distribucion = 'Particulares' then 'Concesionarios' else Distribucion end) as Tipo1,
    (case when Distribucion = 'Particulares' and canaldedistribucion = 'Tradicional' then 'Concesionarios' 
     WHEN distribucion in 
('Particulares','Dexmod') and canaldedistribucion IN ('Moderno','CEDIS') then 'Dexmod' else Distribucion end) as Tipo2,
    (CASE when Distribucion = 'Particulares' and CanalDeDistribucion = 'Tradicional' then 'Concesionarios' 
     when Distribucion IN
    ('Particulares','Dexmod') and CanalDeDistribucion = 'Moderno' then 'Dexmod' else 'Directas' end) as Tipo,
-- DRV --
    (case when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) THEN e.DRV_act  
     when CodigoDeUEN in ('3125','3122','3108','1003','','', '3119','3118') then 'Central-Bajío'    
     when CodigoDeUEN in ('3106','3121','3120') then 'Occidente' WHEN CodigoDeOficinaDeVentas = 'FH21' then 'Central-Bajío'
     when CodigoDeUEN in ('3131','3103') then 'Norte' 
     when Distribucion = 'Particulares' and CodigoDeSolicitanteCorto in (SELECT solicitante from brz_ghq_tech_vertica.concesionarios_drv_act) 
then g.DRV_act else a.DVR end) as DRV_act, 
--CODUEN-
    (CASE when CodigoDeUEN = '3117' and CodigoDeOficinaDeVentas = 'FF11' then '3119' 
     when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) THEN e.UEN  
     when codigodeoficinadeventas  in ( 'FF00', 'FF01' , 'FF06', 'FF07', 'FF08') then '3117' 
     when codigodeoficinadeventas in ( 'GE00', 'GE01','GE03','GE04','GE06' ) then '3119' 
     when codigodeoficinadeventas in ( 'FD04') then '3120' 
     when codigodeoficinadeventas in ( 'GB01', 'GB08') then '3109'
     when codigodeoficinadeventas in ('FP11','FP12','FP13','FP14','FP15','FP17','FP20','FP21','FP23','FP24','FP27','FP28') then '3111_1' 
     when codigodeoficinadeventas in ('FP00','FP04','FP05') then '3111_2' 
     when codigodeoficinadeventas in ('FU05','FU03','FU01','FU00','FU06','FU04','FU02','FE21','FE11','FE08','FE22') then '3104' 
     when codigodeoficinadeventas in ('FE05','FE16','FE03','FE15','FE06','FE17') then '3132'  
     when codigodeoficinadeventas in ( 'FY01') then '3106'
     when a.uen = 'CMM Del Bajío' then '3118' 
     when a.uen = 'Cerveza Canal Mod.' then '3119' 
     when a.uen = '' then '3119' 
     when a.DVR= '' then '3119'
     when codigodeoficinadeventas in ('FP07','FP18','FP08','FP09','FP10','FP29','FP30') then '3111_3' 
     when codigodeoficinadeventas = 'FT01' then '3122' 
     when codigodeoficinadeventas = 'FA00' then '3101' 
     when codigodeoficinadeventas = 'FD05' then '3121' ELSE CodigoDeUEN end) as COD_UEN,
--UEN--
    (case WHEN CodigoDeUEN = '3117' and CodigoDeOficinaDeVentas in ( 'FF11','FH21') then 'CMM Metropolitana'
    when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) THEN e.ORG 
    when codigodeoficinadeventas in ('FU05','FU03','FU01','FU00','FU06','FU04','FU02','FE21','FE11','FE08','FE22') then 'CMM Pacifico Sur' 
    when codigodeoficinadeventas in ('FE05','FE16','FE03','FE15','FE06','FE17') then 'CMM Colima' 
    when codigodeoficinadeventas in ('FP11','FP12','FP13','FP14','FP15','FP17',
'FP20','FP21','FP23','FP24','FP27','FP28') then 'CMM Sureste_Chiapas' 
    when codigodeoficinadeventas in ('FP00','FP04','FP05') then 'CMM Sureste_Yucatan' 
    when codigodeoficinadeventas in ('FP07','FP18','FP08','FP09','FP10','FP29','FP30') then 'CMM Sureste_Quintana Roo' 
    when codigodeoficinadeventas = 'FD05' then 'CMM Guerrero'  
    when a.uen in( 'Cerveza Canal Mod.',' ','')  then 'CMM Metropolitana' 
    when a.codigodeuen in ('3119') then 'CMM Metropolitana'  
    when codigodeoficinadeventas  in ('FF08','FF00','FF01','FF06','FF07') then 'CMM Centro'
    when codigodeoficinadeventas in ('GE01','GE03','GE04','GE06', 'FH21') then 'CMM Metropolitana' 
    when codigodeoficinadeventas = 'FA00' then 'CMM Baja California' 
    when codigodeoficinadeventas = 'FT01' then 'CMM Morelos' else a.UEN end) as UEN,
---- SAG---
    (case when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) THEN e.OFIC 
     WHEN CodigoDeOficinaDeVentas = 'FW01' then 'CMM M Oaxaca' 
     WHEN CodigoDeOficinaDeVentas = 'FH21' then 'CMM M San Pablo' 
     when codigodeoficinadeventas = 'GC08' then 'CMM Tula_H' 
     when codigodeoficinadeventas = 'FV27' then 'CMM Tula_N'
     WHEN CodigoDeOficinaDeVentas = 'FF11' then 'CMM Coacalco Metro' 
     when codigodeuen = 'MC03' then 'Distribuidora Modelo Monclova, S.A. de C.V.'
     when OficinaDeVentas = 'San Jose Cabo URBAN' then 'CMM San Jose Cabo URBAN' 
     when a.CodigoDeDestinatarioCorto in (SELECT codigo from brz_ghq_tech_vertica.estados_sag) then c.Des_SAG 
     when codigodeoficinadeventas = 'FD05' then 'CMM Guerrero'  else OficinaDeVentas end) as SAG, 
--CODSAG--
    (case WHEN CodigoDeOficinaDeVentas = 'FW01' then 'FW00' 
     WHEN CodigoDeOficinaDeVentas = 'FF11' then 'GE04'
     WHEN CodigoDeOficinaDeVentas = 'FH21' then 'GE00' 
     WHEN a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) THEN e.SAG 
     when codigodeoficinadeventas = 'FD05' then 'GA00' else CodigoDeOficinaDeVentas END) as COD_SAG, 
--detallesku--
    a.skucorto as Material ,
    b.Material as Des_Mat,
    b.Marca,
    b.Cupo,
    (CASE WHEN b.Segmento = 'NR' then 'No Retornable' else 'Retornable' END) as Segmento,
    b.Sector,
    b.Innovacion,
    (case when a.codigodedestinatariocorto in (select codigodedestinatariocorto from brz_ghq_tech_vertica.clientes_ba_act) then 'BA' else 'BC' end) as Clasificacion,
    b.Beerhouse,
    b.SubCupo,
    b.Onzas,
    b.PackPresentacion,
    (case WHEN a.Sector = 'Marketplace' then  'Marketplace' else   d.Familia end)  as Familia, 
-- (CASE WHEN a.codigodeoficinadeventas||a.marca in (SELECT * from brz_ghq_tech_vertica.rest_pacifico) then 'No' when a.Marca in (select marca from brz_ghq_tech_vertica.he_marca) then 'Si' else 'No' end) as ZonasCore, 
    (case when b.Importacion = 'Si' and b.Sector in ('Cerveza','Cerveza de Barril') then 'Cerveza Importada' 
     when b.Importacion 
= 'No' and b.Sector in ('Cerveza','Cerveza de Barril') then 'Cerveza Nacional' 
     WHEN b.Marca = 'Red Bull' then 'Bebida Energetizante' 
     when b.Marca = 'Kermato' then 'Jugos' 
     when b.Marca in ('Sta. Maria','NPV','NPV Kids','NPV Sabor','NPV Mineral','Nestle P.V.') then 'No Premium' 
     when b.Marca in ('SFB','Perrier','San Pellegrino','Acqua Panna') then 'Premium' 
     WHEN b.Marca in ('GC Sabor','GC Mineral') then 'Refresco' else 'Revisar' end) as Origen,
    (case when a.sku in (select distinct sku from brz_ghq_tech_vertica.rebaunce) then 'Sí' else 'No' end) as rebaunce,
    ifnull (sum (case when fechadeventa BETWEEN '2021-01-01' and '2021-01-31' then Hectolitros end),0) as Ene_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-02-01' and '2021-02-28' then Hectolitros end),0) as Feb_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-03-01' and '2021-03-31' then Hectolitros end),0) as Mar_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-04-01' and '2021-04-30' then Hectolitros end),0) as Abr_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-05-01' and '2021-05-31' then Hectolitros end),0) as May_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-06-01' and '2021-06-30' then Hectolitros end),0) as Jun_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-07-01' and '2021-07-31' then Hectolitros end),0) as Jul_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-08-01' and '2021-08-31' then Hectolitros end),0) as Ago_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-09-01' and '2021-09-30' then Hectolitros end),0) as Sep_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-10-01' and '2021-10-31' then Hectolitros end),0) as Oct_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-11-01' and '2021-11-30' then Hectolitros end),0) as Nov_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-12-01' and '2021-12-31' then Hectolitros end),0) as Dic_21,
    ifnull (sum (case when fechadeventa BETWEEN '2022-01-01' and '2022-01-31' then Hectolitros end),0) as Ene_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-02-01' and '2022-02-28' then Hectolitros end),0) as Feb_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-03-01' and '2022-03-31' then Hectolitros end),0) as Mar_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-04-01' and '2022-04-30' then Hectolitros end),0) as Abr_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-05-01' and '2022-05-31' then Hectolitros end),0) as May_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-06-01' and '2022-06-30' then Hectolitros end),0) as Jun_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-07-01' and '2022-07-31' then Hectolitros end),0) as Jul_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-08-01' and '2022-08-31' then Hectolitros end),0) as Ago_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-09-01' and '2022-09-30' then Hectolitros end),0) as Sep_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-10-01' and '2022-10-31' then Hectolitros end),0) as Oct_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-11-01' and '2022-11-30' then Hectolitros end),0) as Nov_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-12-01' and '2022-12-31' then Hectolitros end),0) as Dic_22,
    coalesce(sum((CASE when month(fechadeventa)=8 and fechacarga <='2021-08-23' AND year(fechadeventa) =2021 then (Hectolitros) end)),0) as AcumDia_21,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-8-01' and '2022-8-31' then (Hectolitros) end)),0) as AcumDia_22,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2021-8-01' and '2021-8-31' then (Hectolitros) end)),0) as FM_21,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-8-01' and '2022-8-31' then (Hectolitros) end)),0)  /18*27 as Tend,
    coalesce(sum((CASE when month(fechadeventa)<8 AND year(fechadeventa) = 2021 then (Hectolitros) end)),0) + 
    coalesce(sum((CASE when month(fechadeventa)=8 and fechacarga <= '2021-08-23' AND year(fechadeventa) = 2021 then (Hectolitros) end)),0) as YTD_21,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-01-01' and '2022-8-31' then (Hectolitros) end)),0) as YTD_22 
    from brz_ghq_tech_vertica.ventas a 
    LEFT JOIN brz_ghq_tech_vertica.aux_materiales_espejo b on b.SKUCorto=a.SKUCorto 
    LEFT join brz_ghq_tech_vertica.estados_sag c on c.Codigo=a.CodigoDeDestinatarioCorto 
    LEFT JOIN brz_ghq_tech_vertica.familias_v d on d.Marcaver=a.Marca 
    LEFT JOIN brz_ghq_tech_vertica.concesionarios_adquiridos e on e.CLIENTE=a.CodigoDeDestinatarioCorto 
    LEFT JOIN brz_ghq_tech_vertica.concesionarios_drv_act g on a.CodigoDeSolicitanteCorto=g.Solicitante 
    where FechaDeVenta BETWEEN '2021-01-01' and '2022-08-31'
    and codigodeuen not in ('3140','3040') 
    and a.SKUCorto BETWEEN '3000000' AND '3999999' 
    AND a.Documento NOT IN ('ZFCF','ZFNC') 
    AND a.CodigoDeSolicitanteCorto NOT IN ('1003','3040','4502','5001','5003','5501','5502','5503','5504','6001','6501','7006','8003','700000460','700000822','700001238','700001241','700000046','700004194','700001689','700001280') 
    and NumeroDeFactura not in ('0505278711','6014403316') 
    and concat (a.codigodesector, a.distribucion) not in ('2Particulares') 
    AND numerodefactura not in (SELECT DISTINCT numerodefactura from brz_ghq_tech_vertica.facturas_reserva) 
    and codigodeoficinadeventas not in ('O015') 
    AND  distribucion||codigodeuen||YEAR (fechadeventa) not in ('Particulares31402021')
    GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
    UNION
    SELECT DISTINCT distribucion as Tipo1,
    (case when distribucion = 'Concesionarios' then 'Dexmod Conc.' else 'Dexmod Direc.' end ) as Tipo2, 'Dexmod' as Tipo,  
    (case when  org in ('3125','3122','3108','','', '3119','3118') then 'Central-Bajío'    
     when org in ('3106','3121','3120') then 'Occidente' 
     when org in ('3131','3103') then 'Norte'     else drv_act end ) as drv_act, 
--CODUEN--
    (CASE when ID_SAG in ('FP11','FP12','FP13','FP14','FP15','FP17','FP20','FP21','FP23','FP24','FP27','FP28') then '3111_1' 
     when ID_SAG in ('FP00','FP04','FP05') then '3111_2' 
     when ID_SAG  in( 'FF00', 'FF01' , 'FF06', 'FF07', 'FF08') then '3117' 
     when ID_SAG  in ( 'GB01', 'GB08') then '3109' 
     when ID_SAG in ('FU05','FU03','FU01','FU00','FU06','FU04','FU02','FE21','FE11','FE08','FE22') then '3104' 
     when ID_SAG in ('FE05','FE16','FE03','FE15','FE06','FE17') then '3132' 
     when uen = 'CMM Del Bajío' then '3118' 
     when uen = 'Cerveza Canal Mod.' then '3119' 
     when  ID_SAG in ('GE00', 'GE01','GE03','GE04','GE06') then '3119'   
     when ID_SAG in ('FP07','FP18','FP08','FP09','FP10','FP29','FP30') then '3111_3' 
     when ID_SAG = 'FT01'then '3122' WHEN ID_SAG in ('GB01', 'GB08') then '3109' 
     when org = 'CMM Michoacan' then '3106' ELSE org end) as COD_UEN,
--UEN--
    (case when ID_SAG in ('FU05','FU03','FU01','FU00','FU06','FU04','FU02','FE21','FE11','FE08','FE22') then 'CMM Pacifico Sur' 
     when ID_SAG in ('FE05','FE16','FE03','FE15','FE06','FE17') then 'CMM Colima' 
     when ID_SAG in ('FP11','FP12','FP13','FP14','FP15','FP17','FP20','FP21','FP23','FP24','FP27','FP28') then 'CMM Sureste_Chiapas'  
     when uen = 'Cerveza Canal Mod.' then 'CMM Metropolitana' 
     when  ID_SAG in ('GE00','GE01','GE03','GE04','GE06') then 'CMM Metropolitana'
     when ID_SAG in ('FP00','FP04','FP05') then 'CMM Sureste_Yucatan' 
     when ID_SAG in ('FP07','FP18','FP08','FP09','FP10','FP29','FP30') then 'CMM Sureste_Quintana Roo'  
     when ID_SAG = 'FT01'then 'CMM Morelos' WHEN ID_SAG in ('GB01', 'GB08') then 'CMM San Luis Potosí' 
     when ID_SAG in ('FF00','FF01','FF05','FF06','FF07','FF07','FF08','FF09', 'FF10') then 'CMM Centro' else UEN end) as UEN,
--CODSAG--
    (case when SAG = 'CMM SnBartolo Coyot' then 'CMM M Oaxaca' else SAG end) as SAG,
    (case when id_sag = 'FW01' then 'FW00'  else ID_SAG end)AS COD_SAG,
    SKU AS Material,
    b.Material as Des_Mat,
    b.Marca, b.Cupo,
    (CASE WHEN b.Segmento = 'NR' then 'No Retornable' Else 'Retornable' END) as Segmento,
    b.Sector,
    b.Innovacion,
    'BC' as Clasificacion,
    b.Beerhouse,
    b.Subcupo,
    b.Onzas,
    b.PackPresentacion,
    c.Familia  as Familia,
-- (CASE WHEN a.SKU IN ('3004202','3006041','3006043','3006048','3006046','3005856','3005861','3005877','3005882','3005887',
-- '3005892','3005897','3005907','3005912','3005800','3005837','3005850','3006368','3006369','3006370','3006371','3006372',
-- '3006417','3006417','3006547','3005917','3006517') then 'No'
-- when a.Marca in (select marca from brz_ghq_tech_vertica.he_marca) then 'Si' WHEN a.SKU IN ('3005996','3005998') then 'Si'  else 'No'  end) as ZonasCore, 
    (case when b.Importacion = 'Si' and b.Sector in ('Cerveza','Cerveza de Barril') then 'Cerveza Importada' 
     when b.Importacion = 'No' and b.Sector in ('Cerveza','Cerveza de Barril') then 'Cerveza Nacional' 
     WHEN b.Marca = 'Red Bull' then 'Bebida Energetizante' 
     when b.Marca = 'Kermato' then 'Jugos' 
     when b.Marca in ('Sta. Maria','NPV','NPV Kids','NPV Sabor','NPV Mineral','Nestle P.V.') then 'No Premium' 
     when b.Marca in ('SFB','Perrier','San Pellegrino','Acqua Panna') then 'Premium' 
     WHEN b.Marca in ('GC Sabor','GC Mineral') then 'Refresco' else 'Revisar' end) as Origen, 
     (case when a.sku in (select distinct sku from brz_ghq_tech_vertica.rebaunce) then 'Sí' else 'No' end) as rebaunce,
    ifnull (sum (case when fechadeventa BETWEEN '2021-01-01' and '2021-01-31' then HL end),0) as Ene_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-02-01' and '2021-02-28' then HL end),0) as Feb_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-03-01' and '2021-03-31' then HL end),0) as Mar_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-04-01' and '2021-04-30' then HL end),0) as Abr_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-05-01' and '2021-05-31' then HL end),0) as May_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-06-01' and '2021-06-30' then HL end),0) as Jun_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-07-01' and '2021-07-31' then HL end),0) as Jul_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-08-01' and '2021-08-31' then HL end),0) as Ago_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-09-01' and '2021-09-30' then HL end),0) as Sep_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-10-01' and '2021-10-31' then HL end),0) as Oct_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-11-01' and '2021-11-30' then HL end),0) as Nov_21,
    ifnull (sum (case when fechadeventa BETWEEN '2021-12-01' and '2021-12-31' then HL end),0) as Dic_21,
    ifnull (sum (case when fechadeventa BETWEEN '2022-01-01' and '2022-01-31' then HL end),0) as Ene_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-02-01' and '2022-02-28' then HL end),0) as Feb_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-03-01' and '2022-03-31' then HL end),0) as Mar_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-04-01' and '2022-04-30' then HL end),0) as Abr_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-05-01' and '2022-05-31' then HL end),0) as May_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-06-01' and '2022-06-30' then HL end),0) as Jun_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-07-01' and '2022-07-31' then HL end),0) as Jul_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-08-01' and '2022-08-31' then HL end),0) as Ago_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-09-01' and '2022-09-30' then HL end),0) as Sep_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-10-01' and '2022-10-31' then HL end),0) as Oct_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-11-01' and '2022-11-30' then HL end),0) as Nov_22,
    ifnull (sum (case when fechadeventa BETWEEN '2022-12-01' and '2022-12-31' then HL end),0) as Dic_22,
    coalesce(sum((CASE when month(fechadeventa)=8 and fechacarga <='2021-08-23' AND year(fechadeventa) =2021 then(hl) end)),0) as AcumDia_21,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-8-01' and '2022-8-31' then(hl) end)),0) as AcumDia_22, 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2021-8-01' and '2021-8-31' then(hl) end)),0) as MTD_21,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-8-01' and '2022-8-31' then(hl) end)),0)  /18*27 as Tend,
    coalesce(sum((CASE when month(fechadeventa)<8 AND year(fechadeventa) =2021 then(hl) end)),0) + 
    coalesce(sum((CASE when month(fechadeventa)=8 and fechacarga <='2021-08-23'AND year(fechadeventa) =2021 then(hl) end)),0) as YTD_21,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-01-01' and '2022-8-31'then(hl) end)),0) as YTD_22 
    from brz_ghq_tech_vertica.vw_dexmod_2_prueba a 
    LEFT JOIN brz_ghq_tech_vertica.aux_materiales_espejo b on b.SKUCorto=a.SKU 
    LEFT JOIN brz_ghq_tech_vertica.familias_v c on c.Marcaver=a.Marca  
    where FechaDeVenta BETWEEN '2021-01-01' and '2022-08-31' 
    AND org || year (fechadeventa) not in ('31412020')   
    and org not in ('3140','3040') 
    AND distribucion||org||YEAR(fechadeventa) not in ('Particulares31402021') 
    and a.Marca not in ('Mservicio') 
    GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23;

    """
)

#display(df_consum_1)

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

# COMMAND ----------

try:
    df_consum_2 = spark.sql(
    """
    SELECT DISTINCT 
    (case when CodigoDeUEN = '3119' and Distribucion = 'Particulares' then 'Directas' when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) then 'Directas' 
     when Distribucion = 'Particulares' then 'Concesionarios' else Distribucion end) as Tipo,
     (case when CodigoDeUEN = '3119' and Distribucion = 'Particulares' then 'Directas' when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) then 'Directas' 
      when Distribucion = 'Particulares' then 'Concesionarios' else Distribucion end) as Tipo2,
--canal---
    (CASE WHEN A.CODIGODEDESTINATARIOCORTO IN ( SELECT DISTINCT codigodedestinatariocorto from brz_ghq_tech_vertica.clientes_ba_act) then 'BA'
    when a.Distribucion = 'Particulares' then 'Concesionarios'
    WHEN A.CODIGODEDESTINATARIOCORTO IN (SELECT codigodedestinatariocorto FROM brz_ghq_tech_vertica.aux_cliente_np) THEN 'Modeloramas'
    when a.codigodesolicitantecorto in (select codigodesolicitantecorto from brz_ghq_tech_vertica.moderno_m) then 'Moderno'
    else 'Tradicional' end) as Canal,
-- subcanal--
    (case when A.codigodedestinatariocorto in (SELECT CodigoDeDestinatarioCorto from brz_ghq_tech_vertica.clientes_ba_act )
 -- and not in (select distinct cliente from brz_ghq_tech_vertica.segmentacionUC)--
    then 'BA Social'
    when a.Distribucion = 'Particulares' then 'Concesionarios'
    when a.codigodedestinatariocorto in (select distinct cliente from brz_ghq_tech_vertica.segmentacionuc) then 'UC'
    when a.codigodedestinatariocorto in (select codigodedestinatariocorto from brz_ghq_tech_vertica.aux_cliente_np) then 'Modeloramas'
    when a.codigodesolicitantecorto in (select codigodesolicitantecorto from brz_ghq_tech_vertica.moderno_m) then 'Moderno'
    when a.distribucion in ('Particulares','Dexmod') and canaldedistribucion IN ('Moderno','CEDIS') then 'Dexmod' 
    else 'Tradicional' end) as SubCanal,
--DRV
    (case when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) THEN e.DRV_act 
    when CodigoDeUEN in ('3125','3122','3108') then 'Central-Bajío' when CodigoDeUEN in ('3106','3121','3120') then 'Occidente' 
    when CodigoDeUEN in ('3131','3103') then 'Norte'
    when Distribucion = 'Particulares' and CodigoDeSolicitanteCorto in (SELECT solicitante from brz_ghq_tech_vertica.concesionarios_drv_act) then g.DRV_act else dvr end) as DRV,
--COD_UEN--
(CASE when CodigoDeUEN = '3117' and CodigoDeOficinaDeVentas = 'FF11' then '3119' 
 when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) THEN e.UEN 
 when Distribucion = 'Particulares' and a.UEN = 'CMM Metropolitana' then '3119' when Distribucion = 'Particulares' and CanalDeDistribucion = 'Tradicional' then '1003'
    when codigodeoficinadeventas in ('FP11','FP12','FP13','FP14','FP15','FP17','FP20','FP21','FP23','FP24','FP27','FP28') then '3111_1' 
    when codigodeoficinadeventas in ('FU05','FU03','FU01','FU00','FU06','FU04','FU02','FE21','FE11','FE08','FE22') then '3104' 
    when codigodeoficinadeventas in ('FE05','FE16','FE03','FE15','FE06','FE17') then '3132'  
    when codigodeoficinadeventas in ('FP00','FP04','FP05') then '3111_2'
    when codigodeoficinadeventas in ('FP07','FP18','FP08','FP09','FP10','FP29','FP30') then '3111_3' 
    when codigodeoficinadeventas = 'FD05' then '3121' ELSE CodigoDeUEN end) as COD_UEN,
--UEN--
    (case WHEN CodigoDeUEN = '3117' and CodigoDeOficinaDeVentas = 'FF11' then 'CMM Metropolitana'  
    when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos where uen = '3104' ) then 'CMM Pacifico Sur' 
     when a.codigodedestinatariocorto in (SELECT CLIENTE from brz_ghq_tech_vertica.concesionarios_adquiridos) THEN e.ORG 
     when Distribucion = 'Particulares' and CanalDeDistribucion = 'Tradicional' AND codigodesolicitantecorto in (select solicitante from brz_ghq_tech_vertica.concesionarios_drv_act where drv_act = 'Central-Bajío') then 'Conc. Central-Bajío' 
     when Distribucion = 'Particulares' and CanalDeDistribucion = 'Tradicional' AND codigodesolicitantecorto in (select solicitante from brz_ghq_tech_vertica.concesionarios_drv_act where drv_act =  'Norte') then 'Conc. Norte' 
     when Distribucion = 'Particulares' and CanalDeDistribucion = 'Tradicional' AND codigodesolicitantecorto in (select solicitante from brz_ghq_tech_vertica.concesionarios_drv_act where drv_act =  'Occidente') then 'Conc. Occidente' 
     when Distribucion = 'Particulares' and CanalDeDistribucion = 'Tradicional' AND codigodesolicitantecorto in (select solicitante from brz_ghq_tech_vertica.concesionarios_drv_act where drv_act =  'Sureste') then 'Conc. Sureste' 
     when codigodeoficinadeventas in ('FU05','FU03','FU01','FU00','FU06','FU04','FU02','FE21','FE11','FE08','FE22') then 'CMM Pacifico Sur'
     when codigodeoficinadeventas in ('FE05','FE16','FE03','FE15','FE06','FE17') then 'CMM Colima'  
     when codigodeoficinadeventas in ('FP11','FP12','FP13','FP14','FP15','FP17','FP20','FP21','FP23','FP24','FP27','FP28') then 'CMM Sureste_Chiapas'
     when codigodeoficinadeventas in ('FP00','FP04','FP05') then 'CMM Sureste_Yucatan' 
     when codigodeoficinadeventas in ('FP07','FP18','FP08','FP09','FP10','FP29','FP30') then 'CMM Sureste_Quintana Roo' 
     when codigodeoficinadeventas = 'FD05' then 'CMM Guerrero' else a.UEN end) as UEN,
--ID_SAG--
     (case when a.CodigoDeDestinatarioCorto in (select cliente from brz_ghq_tech_vertica.concesionarios_adquiridos) then e.SAG 
     when a.distribucion='Particulares' then '1003'
     when a.codigodeoficinadeventas='FF11' then 'GE04'
     when a.CodigoDeOficinaDeVentas='GA06' then 'GA05' else a.codigodeoficinadeventas end) as ID_SAG,
--SAG--
    (case when a.CodigoDeDestinatarioCorto in (select cliente from brz_ghq_tech_vertica.concesionarios_adquiridos) then e.OFIC 
    when a.distribucion='Particulares' then 'Concesionarios'
    when a.codigodeoficinadeventas='FF11' then 'CMM Coacalco Metro'
    when a.OficinaDeVentas='CMM Taxco' then 'CMM Iguala' else a.OficinaDeVentas end) as SAG,
    a.SKUCorto,a.NombredeMaterial as Material,
    a.Marca, 
    p.Cupo,
    d.Tipo as Segmento,
    f.Familia,
--SEG_HE_NABS--
    (case when a.CodigoDeDestinatarioCorto in (select codigo_sap from brz_ghq_tech_vertica.seg_he)then 'SI'ELSE 'NO' END) as SEG_HE,
    'HE' as SEG_HE_NABS,
--BEES--
    (case when a.NumeroPedidoCliente like ('%B2BMX%') then 'Si' else 'No' end) as Bees,
--HISTORICO--
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=1 then hectolitros else 0 end) as ENE21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=2 then hectolitros else 0 end) as FEB21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=3 then hectolitros else 0 end) as MAR21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=4 then hectolitros else 0 end) as ABR21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=5 then hectolitros else 0 end) as MAY21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=6 then hectolitros else 0 end) as JUN21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=7 then hectolitros else 0 end) as JUL21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=8 then hectolitros else 0 end) as AGO21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=9 then hectolitros else 0 end) as SEP21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=10 then hectolitros else 0 end) as OCT21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=11 then hectolitros else 0 end) as NOV21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=12 then hectolitros else 0 end) as DIC21HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=1 then hectolitros else 0 end) as ENE22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=2 then hectolitros else 0 end) as FEB22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=3 then hectolitros else 0 end) as MAR22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=4 then hectolitros else 0 end) as ABR22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=5 then hectolitros else 0 end) as MAY22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=6 then hectolitros else 0 end) as JUN22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=7 then hectolitros else 0 end) as JUL22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=8 then hectolitros else 0 end) as AGO22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=9 then hectolitros else 0 end) as SEP22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=10 then hectolitros else 0 end) as OCT22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=11 then hectolitros else 0 end) as NOV22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=12 then hectolitros else 0 end) as DIC22HL, 
    coalesce(sum((CASE when MONTH(FechaDeVenta)=8 and FechaCarga<='2021-08-23'  then(Hectolitros) end)),0) as AcumDia_21,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-08-1' and '2022-08-31'  then(Hectolitros) end)),0) as AcumDia_22,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2021-08-1' and '2021-08-31'  then(Hectolitros) end)),0) as Mes_21, 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-08-1' and '2022-08-31'  then(Hectolitros) end)),0) / 18 * 27 as Tend,
    coalesce(sum((CASE when FechaCarga<='2021-08-23'  then(Hectolitros) end)),0) as YTD_21, 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-1-1' and '2022-08-31'  then(Hectolitros) end)),0) as YTD_22
--brz_ghq_tech_vertica.VENTAS--
    from brz_ghq_tech_vertica.ventas a 
    LEFT join brz_ghq_tech_vertica.estados_sag c on c.Codigo=a.CodigoDeDestinatarioCorto 
    LEFT JOIN brz_ghq_tech_vertica.concesionarios_adquiridos e on e.CLIENTE=a.CodigoDeDestinatarioCorto 
    LEFT JOIN brz_ghq_tech_vertica.concesionarios_drv_act g on a.CodigoDeSolicitanteCorto=g.Solicitante
    LEFT JOIN brz_ghq_tech_vertica.aux_materiales_espejo p on a.SKUCorto=p.SKUCorto
    INNER JOIN brz_ghq_tech_vertica.he_marca d on a.Marca=d.Marca
    LEFT JOIN brz_ghq_tech_vertica.familias_v f on a.Marca=f.Marcaver
    where FechaDeVenta BETWEEN '2021-01-01' and '2022-12-31'  
-------------and codigodesector = '1'---------------
    and codigodeuen not in ('3140','3040') 
    and a.SKUCorto BETWEEN '3000000' AND '3999999' 
    AND a.Documento NOT IN ('ZFCF','ZFNC')  
    AND a.CodigoDeSolicitanteCorto NOT IN ('1003','3040','4502','5001','5003','5501','5502','5503','5504','6001','6501','7006','8003','700000460','700000822','700001238','700001241','700000046','700004194','700001689','700001280') 
    and NumeroDeFactura not in ('0505278711','6014403316') 
    and NumeroDeFactura not in (SELECT DISTINCT numerodefactura from brz_ghq_tech_vertica.facturas_reserva)
    and codigodeoficinadeventas not in ('O015')
    and CONCAT (a.CodigoDeOficinaDeVentas,a.Marca) not in (SELECT DISTINCT CODSAGMARCA FROM brz_ghq_tech_vertica.rest_pacifico)
    AND a.MARCA NOT IN ('Budweiser')                                                                                                                                                                                                                                                           
    and distribucion||codigodeuen||YEAR (FechaDeVenta) not in ('Particulares31402021')                                
    GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
    HAVING 
    coalesce(sum((CASE when MONTH(FechaDeVenta)=8 and FechaCarga<='2021-08-23'  then(Hectolitros) end)),0) <> 0 or 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-08-1' and '2022-08-31'  then(Hectolitros) end)),0) <> 0 or 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2021-08-1' and '2021-08-31'  then(Hectolitros) end)),0) <> 0 or
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-08-1' and '2022-08-31'  then(Hectolitros) end)),0) / 18 * 27 <> 0 or 
    coalesce(sum((CASE when FechaCarga<='2021-08-23'  then(Hectolitros) end)),0) <> 0 or 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-1-1' and '2022-08-31'  then(Hectolitros) end)),0) <> 0 
    UNION
    SELECT DISTINCT 
    distribucion as Tipo,
    'Dexmod' as tipo2,
    'Moderno' as Canal,
    'Moderno' as SubCanal,
    DRV_Act as DRV, 
    (case when ID_SAG in ('FP11','FP12','FP13','FP14','FP15','FP17','FP20','FP21','FP23','FP24','FP27','FP28') then '3111_1'
    when ID_SAG in ('FP00','FP04','FP05') then '3111_2' 
    when ID_SAG in ('FU05','FU03','FU01','FU00','FU06','FU04','FU02','FE21','FE11','FE08','FE22') then '3104'  
    when ID_SAG in ('FE05','FE16','FE03','FE15','FE06','FE17') then '3132' 
    when ID_SAG in ('FP07','FP18','FP08','FP09','FP10','FP29','FP30') then '3111_3' 
    when ID_SAG = 'FD05' then '3121' 
    else org end) as COD_UEN,
--UEN—
    (case when DRV_act = 'Central-Bajío' and org = '1003' then 'Conc. Central-Bajío' 
    when DRV_act = 'Norte' and org = '1003' then 'Conc. Norte'  
    when DRV_act = 'Occidente' and org = '1003' then 'Conc. Occidente' 
    when DRV_act = 'Sureste' and org = '1003' then 'Conc. Sureste' when org = '3137' then 'CMM Sureste_Yucatan' when org = '3136' THEN 'CMM Sureste_Quintana Roo' 
    when org = '3135' then 'CMM Sureste_Chiapas' when ID_SAG in ('FP11','FP12','FP13','FP14','FP15','FP17','FP20','FP21','FP23','FP24','FP27','FP28') then 'CMM Sureste_Chiapas'  
    when ID_SAG in ('FU05','FU03','FU01','FU00','FU06','FU04','FU02','FE21','FE11','FE08','FE22') then 'CMM Pacifico Sur'  when ID_SAG in ('FE05','FE16','FE03','FE15','FE06','FE17') then 'CMM Colima' 
    when ID_SAG in ('FP00','FP04','FP05') then 'CMM Sureste_Yucatan'
    when ID_SAG in ('FP07','FP18','FP08','FP09','FP10','FP29','FP30') then 'CMM Sureste_Quintana Roo' 
    when ID_SAG = 'FD05' then 'CMM Guerrero' else uen end) as UEN,
--ID_SAG--
    (case when id_sag='FF11' then 'GE04' 
    when id_sag = 'FW01' then 'FW00' 
    when id_sag = 'GA05' then 'GA06' else ID_SAG end) as ID_SAG,
--SAG--
    (case when SAG = 'CMM SnBartolo Coyot' then 'CMM M Oaxaca' 
    when id_sag='FF11' then 'CMM Coacalco Metro'
    when sag='CMM Taxco' then 'CMM Iguala' else SAG end) as SAG, 
    a.SKU as SKUCorto,
    a.Material,
    a.Marca,
    a.Cupo,
    d.Tipo as Segmento,
    f.Familia,
--SEG_HE—
    'NO'as SEG_HE,
--SEG_HE_NABS—
    'HE' as SEG_HE_NABS,
--BEES--
    'No' as Bees,
--HISTORICO--
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=1 then HL else 0 end) as ENE21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=2 then HL else 0 end) as FEB21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=3 then HL else 0 end) as MAR21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=4 then HL else 0 end) as ABR21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=5 then HL else 0 end) as MAY21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=6 then HL else 0 end) as JUN21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=7 then HL else 0 end) as JUL21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=8 then HL else 0 end) as AGO21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=9 then HL else 0 end) as SEP21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=10 then HL else 0 end) as OCT21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=11 then HL else 0 end) as NOV21HL,
    sum(case when year(fechadeventa)=2021 and month(fechadeventa)=12 then HL else 0 end) as DIC21HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=1 then HL else 0 end) as ENE22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=2 then HL else 0 end) as FEB22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=3 then HL else 0 end) as MAR22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=4 then HL else 0 end) as ABR22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=5 then HL else 0 end) as MAY22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=6 then HL else 0 end) as JUN22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=7 then HL else 0 end) as JUL22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=8 then HL else 0 end) as AGO22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=9 then HL else 0 end) as SEP22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=10 then HL else 0 end) as OCT22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=11 then HL else 0 end) as NOV22HL,
    sum(case when year(fechadeventa)=2022 and month(fechadeventa)=12 then HL else 0 end) as DIC22HL, 
    coalesce(sum((CASE when MONTH(FechaDeVenta)=8 and FechaCarga<='2021-08-23'  then(HL) end)),0) as AcumDia_21,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-08-1' and '2022-08-31'  then(HL) end)),0) as AcumDia_22,
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2021-08-1' and '2021-08-31'  then(HL) end)),0) as Mes_21, 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-08-1' and '2022-08-31'  then(HL) end)),0) / 18 * 27 as Tend,
    coalesce(sum((CASE when FechaCarga<='2021-08-23'  then(HL) end)),0) as YTD_21, 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-1-1' and '2022-08-31'  then(HL) end)),0) as YTD_22
--brz_ghq_tech_vertica.VENTAS—
    from brz_ghq_tech_vertica.vw_dexmod_2_prueba a 
    LEFT JOIN brz_ghq_tech_vertica.aux_materiales_espejo p on a.SKU=p.SKUCorto
    INNER JOIN brz_ghq_tech_vertica.he_marca d on a.Marca=d.Marca
    LEFT JOIN brz_ghq_tech_vertica.familias_v f on a.Marca=f.Marcaver
    where FechaDeVenta BETWEEN '2021-01-01' and '2022-12-31'  
    and a.SKU BETWEEN '3000000' AND '3999999' 
--and id_sag not in ('O015')
    AND a.MARCA NOT IN ('Budweiser')                                                                 
    GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
    HAVING  
    coalesce(sum((CASE when MONTH(FechaDeVenta)=8 and FechaCarga<='2021-08-23'  then(HL) end)),0) <> 0 or 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-08-1' and '2022-08-31'  then(HL) end)),0) <> 0 or 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2021-08-1' and '2021-08-31'  then(HL) end)),0) <> 0 or
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-08-1' and '2022-08-31'  then(HL) end)),0) / 18 * 27 <> 0 or 
    coalesce(sum((CASE when FechaCarga<='2021-08-23'  then(HL) end)),0) <> 0 or 
    coalesce(sum((CASE when FechaDeVenta BETWEEN '2022-1-1' and '2022-08-31'  then(HL) end)),0) <> 0;

    """
)

#display(df_consum_2)

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

# COMMAND ----------

try:
    df_consum_3 = spark.sql(
    """
    (
        SELECT DISTINCT 
        (case
                                   when OrgVt in ('3108','3116','3117','3118','3119','3122','3125') then 'Central-Bajío'
                                   when orgvt in ('3101','3102','3103','3126','3127','3128','3129','3131','3002') then 'Norte'
                                   when orgvt in ('3104','3105','3106','3109','3110','3120','3121') then 'Occidente'
                                   when orgvt in ('3111','3112','3113','3115','3123','3124') then 'Sureste' 
                                   else orgvt end ) DRV,
        est.ORG,
        est.UEN,
        OfVta,
        est.SAG,
        a.Material,
        b.Material as Descripcion_Material,
        (CASE WHEN a.Solic IN ( SELECT DISTINCT codigodedestinatariocorto from brz_ghq_tech_vertica.clientes_ba_act) then 'BA' 
        WHEN a.Solic IN (SELECT codigodedestinatariocorto FROM brz_ghq_tech_vertica.aux_cliente_np) THEN 'Modeloramas'
        when creado_por = 'HECC052S' and clvt = 'Y070' then 'Moderno' 
        when creado_por = 'HECC052S' and clvt = 'Y079' then 'Moderno' ELSE 'Tradicional' END) AS CANAL,
        Creado_el,
        FePrefEnt,
        (case when creado_por = 'CPS_PAC' then 'Preventa' when creado_por = 'DSD_RFCUSR' then 'Preventa' 
        when creado_por = 'HECC008C' then 'Televenta' when creado_por = 'HECC016C' then 'B2B' when Creado_por = 'HECC111C' then 'B2B_Bees'
        when creado_por = 'HECC030C' then 'Televenta' when creado_por = 'HECC052S' and clvt = 'YD48' then 'Devolucion' 
        when creado_por = 'HECC052S' and clvt = 'YD50' then 'Devolucion' when creado_por = 'HECC052S' and clvt = 'YD51' then 'Devolucion' 
        when creado_por = 'HECC052S' and clvt = 'Y070' then 'Moderno' when creado_por = 'HECC052S' and clvt = 'Y079' then 'Moderno' 
        when creado_por = 'HECC052S' and clvt = 'YD13' then 'Moderno' when creado_por = 'HECC052S' and clvt = 'Y050' then 'Autoventa' 
        else 'Preventa' end ) as ModeloServicio, 
        ( CASE when MR in('10','92','CZ','F0','F1','F2','Y2','Y3','Z1','Z2','ZF','ZG','ZH','ZI','ZJ','ZK') then 'Cancelacion' 
        when MR in ('91','93','90','94','95','96','CD','CY','DI','EV','NS','PB','QA','RB','RC') then 'Stock Out' 
        when MR = 'ZD' then 'Duplicados' 
        when MR = 'ZE' then 'Black List'
        when MR = '97' then 'Fecha Prefente' else 'Disponible' end) as MotivodeRechazo,b.sector,a.Solic,
        (case when b.Marca in ('Corona Cero','Corona 1.8','Victoria 1.8','Victoria Chamoy','Victoria Chelada') then 'Beyond Beer' 
        WHEN b.CodigoDeSector in ('2','25','3','23','4') then 'Beyond Beer' 
        WHEN b.CodigoDeSector in ('6') then 'Market Place' else 'Beer' end) as Segmento,
        sum (Volumen)/100 as hl
        from brz_ghq_tech_vertica.cdp_base a
        LEFT join brz_ghq_tech_vertica.aux_materiales_espejo b 
        on a.Material = b.SKUCorto
        left join (SELECT DISTINCT ORG,UEN,ID_SAG,SAG  from brz_ghq_tech_vertica.est_ventas) est 
        on OfVta = est.ID_SAG
        WHERE RIGHT(Creado_el,7) in ('05.2022','06.2022','07.2022','08.2022')
        GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
        UNION
        SELECT DISTINCT 
        (case when OrgVt in ('3108','3116','3117','3118','3119','3122','3125') then 'Central-Bajío'
        when orgvt in ('3101','3102','3103','3126','3127','3128','3129','3131','3002') then 'Norte'
        when orgvt in ('3104','3105','3106','3109','3110','3120','3121') then 'Occidente'
        when orgvt in ('3111','3112','3113','3115','3123','3124') then 'Sureste' else orgvt end ) DRV,
        est.ORG,
        est.UEN,
        OfVta,
        est.SAG,
        a.Material,
        c.Material as Descripcion_Material,
        (CASE WHEN a.Solic IN ( SELECT DISTINCT codigodedestinatariocorto from brz_ghq_tech_vertica.clientes_ba_act) then 'BA'
        WHEN a.Solic IN (SELECT codigodedestinatariocorto FROM brz_ghq_tech_vertica.aux_cliente_np) THEN 'Modeloramas'
        when creado_por = 'HECC052S' and clvt = 'Y070' then 'Moderno' when creado_por = 'HECC052S' and clvt = 'Y079' then 'Moderno' 
        ELSE 'Tradicional' END) AS CANAL,
        Creado_el,
        FePrefEnt,
        (case when creado_por = 'CPS_PAC' then 'Preventa' when creado_por = 'DSD_RFCUSR' then 'Preventa' 
        when creado_por = 'HECC008C' then 'Televenta' when creado_por = 'HECC016C' then 'B2B' when Creado_por = 'HECC111C' then 'B2B_Bees'
        when creado_por = 'HECC030C' then 'Televenta' when creado_por = 'HECC052S' and clvt = 'YD48' then 'Devolucion' 
        when creado_por = 'HECC052S' and clvt = 'YD50' then 'Devolucion' when creado_por = 'HECC052S' and clvt = 'YD51' then 'Devolucion' 
        when creado_por = 'HECC052S' and clvt = 'Y070' then 'Moderno' when creado_por = 'HECC052S' and clvt = 'Y079' then 'Moderno' 
        when creado_por = 'HECC052S' and clvt = 'YD13' then 'Moderno' when creado_por = 'HECC052S' and clvt = 'Y050' then 'Autoventa' 
        else 'Preventa' end ) as ModeloServicio, 
        'Factura' as MotivodeRechazo,
        c.Sector,
        a.Solic,
        (case when c.Marca in ('Corona Cero','Corona 1.8','Victoria 1.8','Victoria Chamoy','Victoria Chelada') then 'Beyond Beer' 
        WHEN c.CodigoDeSector in ('2','25','3','23','4') then 'Beyond Beer' 
        WHEN c.CodigoDeSector in ('6') then 'Market Place' else 'Beer' end) as Segmento,
        sum(case when Docventa = documentoventa and a.Material = b.skucorto then hectolitros else 0 end) as hl 
        from brz_ghq_tech_vertica.cdp_base A
        LEFT JOIN (SELECT DISTINCT (case when documento = 'Y079' then SUBSTRING (documentoventa,2,9) else documentoventa end) as documentoventa,
        skucorto, hectolitros
        from brz_ghq_tech_vertica.ventas 
                   where distribucion = 'Directas' and codigodesector in ('1','2','25','3','23','4') 
                   and skucorto between '3000000' and '3999999' and fechadeventa >= '2021-07-01' ) B ON A.Docventa=b.documentoventa  and a.Material=b.skucorto 
        LEFT join brz_ghq_tech_vertica.aux_materiales_espejo c on a.Material = c.SKUCorto 
        left join (SELECT DISTINCT ORG,UEN,ID_SAG,SAG  from brz_ghq_tech_vertica.est_ventas) est on OfVta = est.ID_SAG 
        WHERE RIGHT (Creado_el,7) in ('05.2022','06.2022','07.2022','08.2022') 
        GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 
        HAVING sum(case when Docventa = documentoventa and a.Material = b.skucorto then hectolitros else 0 end) <> 0);
    """
)

#display(df_consum_3)

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

# COMMAND ----------

row_count = 0
target_object = source_system

# COMMAND ----------

results = common_utils.ReturnObject(
    status=common_utils.RunStatus.SUCCEEDED,
    target_object=target_object,
    num_records_read=row_count,
    num_records_loaded=row_count,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
