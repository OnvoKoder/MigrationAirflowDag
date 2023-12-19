CREATE OR REPLACE PROCEDURE public.insert_product(IN p_product_name character varying,
                                                  IN p_category     character varying,
                                                  IN p_price        numeric,
                                                  IN p_supplier     character varying)
 LANGUAGE plpgsql
AS $procedure$
declare 
l_qnty numeric := 0;
begin
	select count(hp.keyid)
	  into l_qnty
	  from hub_product hp
	 where hp.keyid = sha256(p_product_name::bytea);

	if l_qnty = 0 then
	  insert into hub_product (keyid, load_date) 
	  values (sha256(p_product_name::bytea), localtimestamp);
	   
	  insert into sat_product (product_id, load_date, name_product, category, price, supplier)
	  values (sha256(p_product_name::bytea), localtimestamp, p_product_name, p_category, p_price, p_supplier);
	end if; 
	end;
$procedure$
;