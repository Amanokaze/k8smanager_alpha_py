export const metrics = {
    'overall': [
        'ID','Node','CPU Usage(%)','Memory Used(%)','Swap Used(%)',
        'Memory Size(GB)','Swap Size(GB)','NT Usage(%)',
        'FS Usage(%)',
        'FS Size(GB)',
        'FS Inode Usage(%)',
        'Image Usage(%)',
        'Proc Count'
    ],
    'nodesysco': [
        'Container',
        'CPU Usage(%)',
        'Memory Used(%)',
        'Swap Used(%)',
        'Memory Size(GB)',
        'Swap Size(GB)'
    ], 
    'pod': [
        [
            {'name': 'ID', 'span': 2, 'spanType': 'row'},
            {'name': 'Pod', 'span': 2, 'spanType': 'row'},
            {'name': 'CPU Usage(%)', 'span': 2, 'spanType': 'row'},
            {'name': 'Memory Used(%)', 'span': 2, 'spanType': 'row'},
            {'name': 'Swap Used(%)', 'span': 2, 'spanType': 'row'},
            {'name': 'Memory Size(GB)', 'span': 2, 'spanType': 'row'},
            {'name': 'Swap Size(GB)', 'span': 2, 'spanType': 'row'},
            {'name': 'Network', 'span': 5, 'spanType': 'column'},
            {'name': 'Used Size(Byte)', 'span': 4, 'spanType': 'column'},
            {'name': 'Proc Count', 'span': 2, 'spanType': 'row'}    
        ],
        [
            'Usage(%)',
            'RX Rate(%)',
            'TX Rate(%)',
            'RX Errors',
            'TX Errors',
            'Total Volume',
            'Total Volume Inode',
            'ECDHE Storage',
            'ECDHE Storage Inode'
        ]
    ],
    'container': [
        [
            {'name': 'ID', 'span': 2, 'spanType': 'row'},
            {'name': 'Container', 'span': 2, 'spanType': 'row'},
            {'name': 'CPU Usage(%)', 'span': 2, 'spanType': 'row'},
            {'name': 'Memory Used(%)', 'span': 2, 'spanType': 'row'},
            {'name': 'Swap Used(%)', 'span': 2, 'spanType': 'row'},
            {'name': 'Memory Size(GB)', 'span': 2, 'spanType': 'row'},
            {'name': 'Swap Size(GB)', 'span': 2, 'spanType': 'row'},
            {'name': 'Used Size(Byte)', 'span': 4, 'spanType': 'column'},
        ],
        [
            'Root FS',
            'Root FS Inode',
            'Log FS',
            'Log FS Inode'
        ]
    ],
    'podnet': [
        'Network Name',
        'Usage(%)',
        'RX Rate(%)',
        'TX Rate(%)',
        'RX Errors',
        'TX Errors'
    ],
    'podvol': [
        'Volume Name',
        'Used(Byte)',
        'Inode Used(Byte)'
    ]
}