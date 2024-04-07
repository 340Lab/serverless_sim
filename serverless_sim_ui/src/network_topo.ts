export class UINode{
    constructor(
        public x:number,
        public y:number,
        public zIndex:number,
        public id:number) {
    }
}

export class Topo{
    nodes: Node[]
}