import { Global, Module } from '@nestjs/common';
import { BookSearchCaptureService } from './services/book-search-capture.service';

@Global()
@Module({ providers: [BookSearchCaptureService], exports: [BookSearchCaptureService] })
export class BookSearchCaptureModule {}
