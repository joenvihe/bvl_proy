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

                
