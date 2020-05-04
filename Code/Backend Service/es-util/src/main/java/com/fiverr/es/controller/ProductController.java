package com.an.es.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.an.es.dto.ProductRequest;
import com.an.es.dto.ProductResponse;
import com.an.es.service.SearchService;

@RestController
@RequestMapping("/api")
public class ProductController {

	@Autowired
	SearchService service;

	@CrossOrigin(origins = "*")
	@PostMapping(value = "/product")
	public ProductResponse getPostingsByText(@RequestBody ProductRequest body) {
		return service.getDocuments(body);
	}

}
