import { Module } from '@nestjs/common';
import { BigQueryService } from './app.service';
import { DataController } from './app.controller';

@Module({
  imports: [],
  controllers: [DataController],
  providers: [BigQueryService],
})
export class AppModule {}
