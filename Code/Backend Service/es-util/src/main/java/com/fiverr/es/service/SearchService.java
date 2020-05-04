package com.an.es.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.an.es.dto.OtherStorePrice;
import com.an.es.dto.ProductAttributes;
import com.an.es.dto.ProductRequest;
import com.an.es.dto.ProductResponse;
import com.an.es.util.Constants;

@Service
public class SearchService {

	@Value("${elasticsearch.server.host}")
	private String esHost;
	@Value("${elasticsearch.server.scheme}")
	private String esScheme;
	@Value("${elasticsearch.server.port}")
	private int esPort;
	@Value("${elasticsearch.index.searchfield}")
	private String searchField;
	@Value("${elasticsearch.index.type}")
	private String indexType;
	@Value("${elasticsearch.index.name}")
	private String indexName;
	@Value("${elasticsearch.fetch.size}")
	private int fetchSize;
	@Value("${elasticsearch.pmatch.threshold}")
	private Double pMatchThreshold;
	@Value("${consider.weight.aggregation}")
	private boolean isWeightAggr;
	@Value("${consider.diffstore.aggregation}")
	private boolean isDiffStoreAggr;

	private RestHighLevelClient getClient() {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(esHost, esPort, esScheme)));
		return client;
	}

	public ProductResponse getDocuments(ProductRequest productRequest) {
		System.out.println("#### SEARCH REQUEST ####");
		String text = productRequest.getText();
		Map<String, Object> filters = productRequest.getFilters();
		String sortField = productRequest.getSortBy();
		String sortOrder = productRequest.getSortOrder();

		System.out.println("Search Text: " + text);
		ProductResponse result = new ProductResponse();
		ObjectMapper mapper = new ObjectMapper();
		try {
			RestHighLevelClient client = this.getClient();
			SearchRequest searchRequest = new SearchRequest(indexName);
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			BoolQueryBuilder query = QueryBuilders.boolQuery();
			if (text != null) {
				for (String word : text.split(Constants.GeneralConstants.SPACE)) {
					query.must(QueryBuilders.prefixQuery(searchField, word.toLowerCase()));
				}
			} else {
				query.must(QueryBuilders.matchAllQuery());
			}
			if (filters != null) {
				if (filters.size() > 0) {
					for (String filterName : filters.keySet()) {
						query.must(QueryBuilders.matchQuery(filterName, filters.get(filterName.toLowerCase())));
					}
				}
			}
			System.out.println("Query Generated:" + query);
			sourceBuilder.query(query);
			sourceBuilder.size(fetchSize);
			if (sortField != null && !sortField.equals(Constants.GeneralConstants.EMPTY_STRING)) {
				sourceBuilder.sort(new FieldSortBuilder(sortField).order(SortOrder.fromString(sortOrder)));
			}
			searchRequest.source(sourceBuilder);
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			client.close();
			List<ProductAttributes> dataPoints = new ArrayList<>();

			for (SearchHit searchHit : searchResponse.getHits().getHits()) {
				ProductAttributes data = mapper.readValue(searchHit.getSourceAsString(), ProductAttributes.class);
				dataPoints.add(data);
			}
			result.setData(updateDatapoints(dataPoints));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	private Set<ProductAttributes> updateDatapoints(List<ProductAttributes> dataPoints) {
		Set<ProductAttributes> finalList = new HashSet<>();

		Map<Long, String> prodNames = new HashMap<>();
		for (ProductAttributes pa : dataPoints) {
			prodNames.put(pa.getUsid(), pa.getProduct_name());
		}
		System.out.println("USID found with matching text:" + prodNames.keySet());
		Set<Long> keys = new HashSet<>(prodNames.keySet());
		Set<Long> keysR = new HashSet<>();
		int initalKeyCount = keys.size();

		for (Long key : prodNames.keySet()) {
			ProductAttributes p1 = dataPoints.stream().filter(t -> t.getUsid().equals(key)).findFirst().get();
			if (initalKeyCount == 1) {
				finalList.add(p1);
				keys.remove(key);
				keysR.add(key);
			}
			for (Long key1 : prodNames.keySet()) {
				if (keys.contains(key1) && keys.contains(key)) {
					if (!key1.equals(key)) {
						String listOneProduct = prodNames.get(key).replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase();
						String listTwoProduct = prodNames.get(key1).replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase();
						List<String> listOne = Arrays.asList(listOneProduct.split(Constants.GeneralConstants.SPACE));
						List<String> listTwo = Arrays.asList(listTwoProduct.split(Constants.GeneralConstants.SPACE));
						Collection<String> similar = new HashSet<String>(listOne);
						similar.retainAll(listTwo);
						Double percMatch = ((double) similar.size()
								/ Math.min((double) listOne.size(), (double) listTwo.size())) * 100;
						ProductAttributes p2 = dataPoints.stream().filter(t -> t.getUsid().equals(key1)).findFirst()
								.get();
						boolean isWeightEqual = true;
						if (isWeightAggr) {
							isWeightEqual = p1.getProduct_weight().equals(p2.getProduct_weight());
						}
						boolean isDiffStore = true;
						if (isDiffStoreAggr) {
							isDiffStore = !p1.getStore_name().equals(p2.getStore_name());
						}
						// CHECK IF percentage match > 80% AND weights match && different stores
						if ((percMatch > pMatchThreshold) && (isWeightEqual) && (isDiffStore)) {
							Set<OtherStorePrice> ots = new HashSet<>();
							OtherStorePrice ot = new OtherStorePrice();
							if (p1.getNull_count() > p2.getNull_count()) {
								ot.setStore_name(p1.getStore_name());
								ot.setProduct_price(p1.getProduct_price_1());
								ot.setProduct_url(p1.getProduct_url());
								ot.setProduct_name(p1.getProduct_name());
								ot.setUsid(p1.getUsid());
								ot.setProduct_availability(p1.getProduct_availability());
								ots.add(ot);
								keys.remove(key);
								keysR.add(key);
								keysR.add(key1);
								try {
									ProductAttributes pfExists = finalList.stream()
											.filter(t -> t.getUsid().equals(key1)).findFirst().get();
									finalList.remove(pfExists);
									pfExists.getOther_store_prices().addAll(ots);
									finalList.add(pfExists);
								} catch (Exception e) {
									p2.setOther_store_prices(ots);
									finalList.add(p2);
								}
							} else {
								keys.remove(key1);
								keysR.add(key);
								keysR.add(key1);
								ot.setStore_name(p2.getStore_name());
								ot.setProduct_price(p2.getProduct_price_1());
								ot.setProduct_url(p2.getProduct_url());
								ot.setProduct_name(p2.getProduct_name());
								ot.setUsid(p2.getUsid());
								ot.setProduct_availability(p1.getProduct_availability());
								ots.add(ot);
								try {
									ProductAttributes pfExists = finalList.stream().filter(t -> t.getUsid().equals(key))
											.findFirst().get();
									finalList.remove(pfExists);
									pfExists.getOther_store_prices().addAll(ots);
									finalList.add(pfExists);
								} catch (Exception e) {
									p1.setOther_store_prices(ots);
									finalList.add(p1);
									keysR.add(key);
								}
							}
						} else {
							if (!keysR.contains(key)) {
								finalList.add(p1);
								keysR.add(key);
							}
						}
					}
				} else {
					if (!keysR.contains(key)) {
						finalList.add(p1);
						keysR.add(key);
					}
				}
			}
		}
		return finalList;
	}

}
