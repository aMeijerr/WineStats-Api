import { Controller, Get, Query } from '@nestjs/common';
import { BigQuery } from '@google-cloud/bigquery';

@Controller()
export class DataController {
  options = {
    projectId: 'sb-charts',
    keyFilename: './sb-charts-3e4ae4a01d23.json',
  };

  bigQuery = new BigQuery(this.options);
  dataset = this.bigQuery.dataset('SB_totals');
  table = this.dataset.table('sb_data_mars');

  // @Get('product_group')
  // async getProductGroup(@Query('product_group') product_group: string) {
  //   const query = `SELECT date, SUM(sales) AS total_sales FROM ${this.table.id} WHERE product_group = '${product_group}'`;
  //   const [rows] = await this.table.query(query.toString());
  //   return rows;
  // }

  @Get('country')
  async getCountries(
    @Query('country') country: string | undefined,
    @Query('region') region: string | undefined,
    @Query('category') category: string[] | undefined,
    @Query('minYear') minYear: number,
    @Query('maxYear') maxYear: number,
  ) {
    let query = `SELECT date, SUM(sales) AS total_sales FROM ${this.table.id}`;
    let dateFilter = '';
    if (country) {
      dateFilter += ` country = '${country}' AND`;
    }
    if (minYear && maxYear) {
      dateFilter += ` date BETWEEN ${minYear} AND ${maxYear} AND`;
    }
    if (region) {
      dateFilter += ` region = '${region}' AND`;
    }
    if (category) {
      dateFilter += ` product_group = '${category}' AND`;
    }

    if (dateFilter) {
      dateFilter = ` WHERE ${dateFilter.slice(0, -4)}`;
      query += `${dateFilter} GROUP BY date ORDER BY date`;
    } else {
      query += ` GROUP BY date ORDER BY date`;
    }
    const [rows] = await this.table.query(query.toString());
    return rows;
  }
}
