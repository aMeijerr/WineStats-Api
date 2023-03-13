import { Injectable } from '@nestjs/common';
import { BigQuery } from '@google-cloud/bigquery';

@Injectable()
export class BigQueryService {
  private readonly bigQuery: BigQuery;

  constructor() {
    this.bigQuery = new BigQuery();
  }

  async executeQuery(query: string): Promise<any> {
    const options = {
      query: query,
      useLegacySql: false,
    };
    const [job] = await this.bigQuery.createQueryJob(options);
    const [rows] = await job.getQueryResults();
    return rows;
  }
}
