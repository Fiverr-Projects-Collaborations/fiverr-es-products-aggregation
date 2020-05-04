package com.an.es.dto;

import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ProductRequest {

	private String text;
	private String sortBy;
	private String sortOrder;
	private Map<String, Object> filters;

}
