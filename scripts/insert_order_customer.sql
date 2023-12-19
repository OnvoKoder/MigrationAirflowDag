CREATE OR REPLACE PROCEDURE public.insert_order_customer(IN p_order_name character varying,
                                                         IN p_customer character varying)
 LANGUAGE plpgsql
AS $procedure$
declare 
l_qnty numeric := 0;
begin
	select count(*)
	  into l_qnty
	  from link_order_customer lo
	 where lo.order_id = sha256(p_order_name::bytea) 
	   and lo.customer_id = sha256(p_customer::bytea);

	if l_qnty = 0 then
	  insert into link_order_customer (order_id, customer_id, load_date)
	  values (sha256(p_order_name::bytea), sha256(p_customer::bytea), localtimestamp);
	end if; 
	end;
$procedure$;