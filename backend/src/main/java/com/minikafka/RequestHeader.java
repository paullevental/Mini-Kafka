package com.minikafka;


// 8-byte request header. Correlation id is client-chosen, echoed back in the response.
public record RequestHeader(short apiKey, short apiVersion, int correlationId) {}
