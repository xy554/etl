package com.xy.etl.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DirectDataSourceConfigDTO {
    private String sourceName;
    private String jdbcUrl;
    private String jdbcDriver;
    private String jdbcUsername;
    private String jdbcPassword;
    private Integer connectionPoolSize;
}
