import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
export default defineConfig({plugins:[vue()],build:{outDir:'../dist',emptyOutDir:true},server:{host:'127.0.0.1',port:3000,proxy:{'/api':'http://127.0.0.1:8788'}}});
