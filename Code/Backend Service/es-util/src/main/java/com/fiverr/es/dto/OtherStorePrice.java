package com.an.es.dto;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class OtherStorePrice {
	Long usid;
	String product_name;
	String store_name;
	String product_url;
	Float product_price;
	String product_availability;
}
