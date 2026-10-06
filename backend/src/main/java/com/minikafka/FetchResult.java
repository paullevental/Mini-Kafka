package com.minikafka;

import java.util.List;

public record FetchResult(List<Record> records, long endOffset) {}