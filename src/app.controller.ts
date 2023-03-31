import { Controller, Get, Query } from '@nestjs/common';
import { BigQuery } from '@google-cloud/bigquery';

@Controller()
export class DataController {
  options = {
    projectId: 'sb-charts',
  };

  bigQuery = new BigQuery(this.options);
  dataset = this.bigQuery.dataset('SB_totals');
  table = this.dataset.table('sb_data_mars');

  @Get('producerToplist')
  async getProducerTopList(
    @Query('country') country: string | undefined,
    @Query('region') region: string | undefined,
    @Query('category') category: string | undefined,
    @Query('minYear') minYear: number,
    @Query('maxYear') maxYear: number,
  ) {
    let producer_query = `SELECT producer_name, SUM(sales) AS total_sales FROM ${this.table.id}`;
    let dateFilter = '';
    if (country) {
      dateFilter += ` country = '${country}' AND`;
    }
    if (region) {
      dateFilter += ` region = '${region}' AND`;
    }
    if (minYear && maxYear) {
      dateFilter += ` date BETWEEN ${minYear} AND ${maxYear} AND`;
    }
    if (category) {
      dateFilter += ` product_group = '${category}' AND`;
    }
    if (dateFilter) {
      dateFilter = ` WHERE ${dateFilter.slice(0, -4)}`;
      producer_query += `${dateFilter} GROUP BY producer_name ORDER BY total_sales DESC LIMIT 10`;
    } else {
      producer_query += ` GROUP BY producer_name ORDER BY total_sales DESC LIMIT 10`;
    }
    const [rows] = await this.table.query(producer_query.toString());
    return rows;
  }

  @Get('productToplist')
  async getProductTopList(
    @Query('country') country: string | undefined,
    @Query('region') region: string | undefined,
    @Query('category') category: string | undefined,
    @Query('minYear') minYear: number,
    @Query('maxYear') maxYear: number,
  ) {
    let product_query = `SELECT name, SUM(sales) AS total_sales FROM ${this.table.id}`;
    let dateFilter = '';
    if (country) {
      dateFilter += ` country = '${country}' AND`;
    }
    if (region) {
      dateFilter += ` region = '${region}' AND`;
    }
    if (minYear && maxYear) {
      dateFilter += ` date BETWEEN ${minYear} AND ${maxYear} AND`;
    }
    if (category) {
      dateFilter += ` product_group = '${category}' AND`;
    }

    if (dateFilter) {
      dateFilter = ` WHERE ${dateFilter.slice(0, -4)}`;
      product_query += `${dateFilter} GROUP BY name ORDER BY total_sales DESC LIMIT 10`;
    } else {
      product_query += ` GROUP BY name ORDER BY total_sales DESC LIMIT 10`;
    }
    const [rows] = await this.table.query(product_query.toString());
    return rows;
  }

  @Get('categoryToplist')
  async getCategoryTopList(
    @Query('country') country: string | undefined,
    @Query('region') region: string | undefined,
    @Query('category') category: string | undefined,
    @Query('minYear') minYear: number,
    @Query('maxYear') maxYear: number,
  ) {
    let categories_query = `SELECT product_group_detail, SUM(sales) AS total_sales FROM ${this.table.id}`;
    let dateFilter = '';
    if (country) {
      dateFilter += ` country = '${country}' AND`;
    }
    if (region) {
      dateFilter += ` region = '${region}' AND`;
    }
    if (minYear && maxYear) {
      dateFilter += ` date BETWEEN ${minYear} AND ${maxYear} AND`;
    }
    if (category) {
      dateFilter += ` product_group = '${category}' AND`;
    }

    if (dateFilter) {
      dateFilter = ` WHERE ${dateFilter.slice(0, -4)}`;
      categories_query += `${dateFilter} GROUP BY product_group_detail ORDER BY total_sales DESC LIMIT 10`;
    } else {
      categories_query += ` GROUP BY product_group_detail ORDER BY total_sales DESC LIMIT 10`;
    }
    const [rows] = await this.table.query(categories_query.toString());
    return rows;
  }

  @Get('countryTopList')
  async getCountryTopList(
    @Query('country') country: string | undefined,
    @Query('region') region: string | undefined,
    @Query('category') category: string | undefined,
    @Query('minYear') minYear: number,
    @Query('maxYear') maxYear: number,
  ) {
    let country_query = `SELECT country, SUM(sales) AS total_sales FROM ${this.table.id}`;
    let dateFilter = '';
    if (country) {
      dateFilter += ` country = '${country}' AND`;
    }
    if (region) {
      dateFilter += ` region = '${region}' AND`;
    }
    if (minYear && maxYear) {
      dateFilter += ` date BETWEEN ${minYear} AND ${maxYear} AND`;
    }
    if (category) {
      dateFilter += ` product_group = '${category}' AND`;
    }

    if (dateFilter) {
      dateFilter = ` WHERE ${dateFilter.slice(0, -4)}`;
      country_query += `${dateFilter} GROUP BY country ORDER BY total_sales DESC LIMIT 10`;
    } else {
      country_query += ` GROUP BY country ORDER BY total_sales DESC LIMIT 10`;
    }
    const [rows] = await this.table.query(country_query.toString());
    return rows;
  }

  @Get('sales')
  async getSales(
    @Query('country') country: string | undefined,
    @Query('region') region: string | undefined,
    @Query('category') category: string | undefined,
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

//Example on making a dynamic query with switch case, might implement this later
// @Get('topList')
// async getTopList(
//   @Query('country') country: string | undefined,
//   @Query('region') region: string | undefined,
//   @Query('category') category: string | undefined,
//   @Query('minYear') minYear: number,
//   @Query('maxYear') maxYear: number,
//   @Query('type') type: string, // specify the type of top list to return
// ) {
//   let query = '';
//   let groupBy = '';

//   switch (type) {
//     case 'producer':
//       query = `SELECT producer_name, SUM(sales) AS total_sales FROM ${this.table.id}`;
//       groupBy = 'producer_name';
//       break;
//     case 'product':
//       query = `SELECT name, SUM(sales) AS total_sales FROM ${this.table.id}`;
//       groupBy = 'name';
//       break;
//     case 'category':
//       query = `SELECT product_group_detail, SUM(sales) AS total_sales FROM ${this.table.id}`;
//       groupBy = 'product_group_detail';
//       break;
//     case 'country':
//       query = `SELECT country, SUM(sales) AS total_sales FROM ${this.table.id}`;
//       groupBy = 'country';
//       break;
//     default:
//       throw new BadRequestException('Invalid top list type');
//   }

//   let dateFilter = '';
//   if (country) {
//     dateFilter += ` country = '${country}' AND`;
//   }
//   if (region) {
//     dateFilter += ` region = '${region}' AND`;
//   }
//   if (minYear && maxYear) {
//     dateFilter += ` date BETWEEN ${minYear} AND ${maxYear} AND`;
//   }
//   if (category) {
//     dateFilter += ` product_group = '${category}' AND`;
//   }

//   if (dateFilter) {
//     dateFilter = ` WHERE ${dateFilter.slice(0, -4)}`;
//     query += `${dateFilter} GROUP BY ${groupBy} ORDER BY total_sales DESC LIMIT 10`;
//   } else {
//     query += ` GROUP BY ${groupBy} ORDER BY total_sales DESC LIMIT 10`;
//   }

//   const [rows] = await this.table.query(query.toString());
//   return rows;
// }
