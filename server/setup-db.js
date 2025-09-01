const Database = require('./database');
const bcrypt = require('bcryptjs');

async function setupDatabase() {
    console.log('Setting up database...');
    
    const db = new Database();
    
    // Wait for database initialization
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    console.log('Database initialized successfully');
    console.log('Default admin user created: admin / changeme');
    
    // Create some sample data for testing
    if (process.env.NODE_ENV !== 'production') {
        console.log('Adding sample data for testing...');
        
        // Add sample targets
        const sampleTargets = [
            { ip: '192.168.1.100', port: 3389 },
            { ip: '192.168.1.101', port: 3389 },
            { ip: '192.168.1.102', port: 3389 }
        ];
        
        await db.addTargets(sampleTargets);
        
        // Add sample credentials
        const sampleUsers = ['administrator', 'admin', 'user'];
        const samplePasswords = ['password', '123456', 'admin'];
        
        await db.addCredentials(sampleUsers, samplePasswords);
        
        console.log('Sample data added');
    }
    
    db.close();
    console.log('Database setup complete');
}

setupDatabase().catch(console.error);