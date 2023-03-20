import { Module } from '@nestjs/common';
import { BigQueryService } from './app.service';
import { DataController } from './app.controller';
import { ConfigModule } from '@nestjs/config';

@Module({
  imports: [ConfigModule.forRoot()],
  controllers: [DataController],
  providers: [BigQueryService],
})
export class AppModule {}
