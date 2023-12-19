CREATE OR REPLACE PROCEDURE public.insert_order(IN p_order_name character varying,
                                                IN p_order_date timestamp without time zone,
                                                IN p_freight numeric)
 LANGUAGE plpgsql
AS $procedure$
declare 
l_qnty numeric := 0;
begin
	select count(ho.keyid)
	  into l_qnty
	  from hub_orders ho
	 where ho.keyid = sha256(p_order_name::bytea);

	if l_qnty = 0 then
	  insert into hub_orders (keyid, load_date) 
	  values (sha256(p_order_name::bytea), localtimestamp);
	   
	  insert into sat_orders (order_id, load_date, order_date, freight)
	  values (sha256(p_order_name::bytea), localtimestamp, p_order_date, p_freight);
	end if; 
	end;
$procedure$;
