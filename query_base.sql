
## creacion de la tabla tipos_bcrp
CREATE TABLE tipos_bcrp (
	periodo VARCHAR NOT NULL,
	codigo VARCHAR NULL,
	descripcion VARCHAR NULL  
);
commit;



CREATE TABLE stockistoday (
companyCode VARCHAR NULL,
companyName VARCHAR NULL,
shortName VARCHAR NULL,
nemonico VARCHAR NULL,
sectorCode VARCHAR NULL,
sectorDescription VARCHAR NULL,
lastDate VARCHAR NULL,
previousDate VARCHAR NULL,
buy VARCHAR NULL,
sell VARCHAR NULL,
previous VARCHAR NULL,
negotiatedQuantity VARCHAR NULL,
negotiatedAmount VARCHAR NULL,
negotiatedNationalAmount VARCHAR NULL,
operationsNumbe VARCHAR NULL,
currency VARCHAR NULL,
unity VARCHAR NULL,
segment VARCHAR NULL,
createdDate VARCHAR NULL
);
commit;


## creacion de la tabla valor_bcrp
CREATE TABLE valor_bcrp (
	codigo VARCHAR NULL,
	periodo VARCHAR NOT NULL,
	valor VARCHAR NULL  
);
commit;



## creacion de la tabla doc_financieros
CREATE TABLE doc_financieros (
	yearPeriod VARCHAR NOT NULL,
	period VARCHAR NULL,
	documentName VARCHAR NULL,
	documentOrder VARCHAR NULL,
	documentType VARCHAR NULL,
	path VARCHAR NULL,
	rpjCode VARCHAR NULL,
	eeffType VARCHAR NULL,
	caccount VARCHAR NULL, 
	mainTitle VARCHAR NULL,
	numberColumns VARCHAR NULL,
	title VARCHAR NULL,
	value1 VARCHAR NULL
);
commit;

## creacion de la tabla ratios_financieros
CREATE TABLE ratios_financieros (
	codigo VARCHAR NOT NULL,
	dRatio VARCHAR NULL,
	year VARCHAR NULL,
	nImporteA VARCHAR NULL  
);
commit;

## creacion de la tabla noticias
CREATE TABLE noticias (
	periodico VARCHAR NOT NULL,
	seccion VARCHAR NULL,
	_id VARCHAR NULL,
	canonical_url VARCHAR NULL,
	display_date VARCHAR NULL,
	headlines_basic VARCHAR NULL,
	subheadlines_basic VARCHAR NULL,
	taxonomy_seo_keywords VARCHAR NULL,
	taxonomy_tags VARCHAR NULL,
	_type VARCHAR NULL  
);
commit;

### creacion de una tabla para colocar los beneficios
CREATE TABLE stockcompanyvalue(
codigo VARCHAR NULL,
nemonico VARCHAR NULL,
benefitValue VARCHAR NULL,
benefitType VARCHAR NULL,
isin VARCHAR NULL,
dateEntry VARCHAR NULL,
dateAgreement VARCHAR NULL,
dateCut VARCHAR NULL,
dateRegistry VARCHAR NULL,
dateDelivery VARCHAR NULL,
coin VARCHAR NULL,
secMovBe VARCHAR NULL,
secMovDi VARCHAR NULL,
notesValue VARCHAR NULL,
notesLaw VARCHAR NULL,
notesAgreement VARCHAR NULL,
notesCut VARCHAR NULL,
notesRegistry VARCHAR NULL,
notesDelivery VARCHAR NULL
);
commit;



select v.companycode, v.companyname, 
	   v.nemonico, v.sectorcode,
       v.sectordescription, v."rpjCode", 
	   v.website, v."esActDescription",
	   v.coin,v.anho,
	   sum(valor)
from
(
	select a.companycode, a.companyname, 
		   a.nemonico, a.sectorcode,
      		a.sectordescription, a."rpjCode", 
	   		a.website, a."esActDescription",
	 		s.coin,
	   		cast(s.benefitvalue as float) as valor,
	   		s.datecut,
	   		EXTRACT(YEAR FROM to_date(s.datecut, 'YYYY-MM-DD')) as anho,
	   		EXTRACT(MONTH FROM to_date(s.datecut, 'YYYY-MM-DD')) as mes
	from 
		public.companystock a,
		public.stockcompanyvalue s
	where a."rpjCode" is not null and 
		  s.codigo = a.companycode and
	  		s.benefittype = 'DE'
	  		and s.benefitvalue != 'None'
	order by a.sectorcode 
) v
group by v.companycode, v.companyname, 
	     v.nemonico, v.sectorcode,
       v.sectordescription, v."rpjCode", 
	   v.website, v."esActDescription",
	   v.coin,v.anho
order by v.sectorcode,v.nemonico,v.coin,v.anho


select *
from public.stockcompanyvalue s

select *
from public.stockhistory
where nemonico = 'BAP' and average != '0.0'


SELECT yearperiod, period, documentname, documentorder, documenttype, path, rpjcode, 
        eefftype, caccount, maintitle, numbercolumns, title, value1
FROM public.doc_financieros
ORDER BY yearperiod DESC, period DESC

select * from ratios_financieros

select * from doc_financieros

delete from ratios_financieros where 1=1;commit;




select count(*) from valor_bcrp

select a.codigo, a.periodo , a.valor, CAST(CAST(a.fecha AS DATE) AS varchar) fecha
from
(
select  codigo, periodo, valor,
	CASE 
	WHEN length(periodo) = 8 THEN TO_DATE(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(periodo, 'Sep', '01.09'), 'Ago', '01.08'), 'Jul', '01.07'), 'Jun', '01.06'), 'May', '01.05'), 'Abr', '01.04'), 'Mar', '01.03'), 'Feb', '01.02'), 'Ene', '01.01'), 'Dic', '01.12'), 'Nov', '01.11'), 'Oct', '01.10'), 'DD.MM.YY') + interval '1 month'
	WHEN length(periodo) = 5 THEN TO_DATE(replace(replace(replace(replace(periodo, 'T1', '01.01'), 'T2', '01.04'), 'T3', '01.07'), 'T4', '01.10'),'DD.MM.YY') + interval '3 month'
	ELSE TO_DATE(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(periodo, 'Set', 'September'), 'Ago', 'August'), 'Jul', 'July'), 'Jun', 'June'), 'May', 'May'), 'Abr', 'April'), 'Mar', 'March'), 'Feb', 'February'), 'Ene', 'January'), 'Dic', 'December'), 'Nov', 'November'), 'Oct', 'October'), 'dd.Month.YY')+1 
	END AS fecha
-- select *  --,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(periodo, 'Sep', '01.09'), 'Ago', '01.08'), 'Jul', '01.07'), 'Jun', '01.06'), 'May', '01.05'), 'Abr', '01.04'), 'Mar', '01.03'), 'Feb', '01.02'), 'Ene', '01.01'), 'Dic', '01.12'), 'Nov', '01.11'), 'Oct', '01.10')
from valor_bcrp 
-- where codigo = 'PN01510BM' -- and periodo Like '%Oct%'
-- 	where codigo = 'PD38026MD'
where codigo = 'PD37940PQ'
order by 4 desc
limit 1
) a

delete  from valor_bcrp where codigo='PD37940PQ'
delete from tipos_bcrp where codigo ='PD37940PQ'

select * from doc_financieros limit 100

select * from tipos_bcrp


select distinct title
from doc_financieros where lower(title) like '%prec%'
