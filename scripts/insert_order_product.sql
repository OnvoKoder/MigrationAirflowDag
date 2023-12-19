CREATE OR REPLACE PROCEDURE public.insert_order_product(IN p_order_name character varying,
                                                        IN p_product    character varying,
                                                        IN p_qnty       smallint,
                                                        IN p_price      numeric)
 LANGUAGE plpgsql
AS $procedure$
declare 
l_qnty numeric := 0;
begin
	select count(*)
	  into l_qnty
	  from link_orderdetails lo
	 where lo.order_id = sha256(p_order_name::bytea) 
	   and lo.product_id = sha256(p_product::bytea);

	if l_qnty = 0 then
	  insert into link_orderdetails (order_id, product_id, load_date, qnty, price)
	  values (sha256(p_order_name::bytea), sha256(p_product::bytea), localtimestamp, p_qnty, p_price);
	end if; 
	end;
$procedure$;