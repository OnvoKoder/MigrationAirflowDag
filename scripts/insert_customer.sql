CREATE OR REPLACE PROCEDURE public.insert_customer(IN p_customer character varying,
                                                   IN p_staff    character varying,
                                                   IN p_city     character varying,
                                                   IN p_country  character varying,
                                                   IN p_phone    character varying)
 LANGUAGE plpgsql
AS $procedure$
declare 
l_qnty numeric := 0;
begin
	select count(hc.keyid)
	  into l_qnty
	  from hub_customer hc
	 where hc.keyid = sha256(p_customer::bytea);

	if l_qnty = 0 then
	  insert into hub_customer (keyid, load_date) 
	  values (sha256(p_customer::bytea), localtimestamp);
	   
	  insert into sat_customer (customer_id, load_date, firstname, staff, city, country, phone)
	  values (sha256(p_customer::bytea), localtimestamp, p_customer, p_staff, p_city, p_country, p_phone);
	end if; 
	end;
$procedure$;