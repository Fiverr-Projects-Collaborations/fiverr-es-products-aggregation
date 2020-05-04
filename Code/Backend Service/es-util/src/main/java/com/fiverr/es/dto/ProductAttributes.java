package com.an.es.dto;

import java.util.Set;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class ProductAttributes {
	Long usid;
	String product_name;
	String product_url;
	String product_description;
	String product_availability;
	String product_added;
	String product_updated;
	String store_name;
	String productcode_ean;
	String productcode_isbn;
	String productcode_mpn;
	String productcode_sku;
	String productcode_vpc;
	String product_color;
	String product_size;
	String product_weight;
	String image_available;
	String image_stored;
	String image_changed;
	String product_imageurl1;
	String product_imageurl2;
	String product_imageurl3;
	String product_imageurl4;
	String product_imageurl5;
	String img_1;
	String img_2;
	String img_3;
	String img_4;
	String img_5;
	String product_category_1;
	String product_category_2;
	String product_category_3;
	String product_category_4;
	String product_category_5;
	String time_added_category;
	Float product_price_1;
	Float product_price_2;
	Float product_price_3;
	Float product_price_4;
	Float product_price_5;
	String time_added_price;
	Long null_count;
	Set<OtherStorePrice> other_store_prices;
}
