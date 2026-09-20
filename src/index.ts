export type Frame={fin:boolean;opcode:number;payload:Uint8Array};
export function decodeFrame(input:Uint8Array):Frame|null{if(input.length<2)return null;const length=input[1]&127;if(input.length<2+length)return null;return{fin:Boolean(input[0]&128),opcode:input[0]&15,payload:input.slice(2,2+length)}}
export class ConnectionState{state:'open'|'closing'|'closed'='open';close(){if(this.state==='open')this.state='closing'}receiveClose(){this.state='closed'}}
