package com.an.es.enums;

public enum SortOrder {
	ASC {
		public String toString() {
			return "asc";
		}
	},
	DESC {
		public String toString() {
			return "desc";
		}
	};
}