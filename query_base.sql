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


                
select a.companycode, a.companyname, 
	   a.nemonico, a.sectorcode,
       a.sectordescription, a."rpjCode", 
	   a.website, a."esActDescription",
	   s.coin,s.benefitvalue,s.datecut
from 
	public.companystock a,
	public.stockcompanyvalue s
where a."rpjCode" is not null and 
	  s.codigo = a.companycode and
	  s.benefittype = 'DE'
order by a.sectorcode 

select *
from public.stockcompanyvalue s

select *
from public.stockhistory
where nemonico = 'BAP' and average != '0.0'