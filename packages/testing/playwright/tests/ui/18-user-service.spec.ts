import { expect, test } from '../../fixtures/base';

test.describe('User API Service', () => {
	test('should create a user with default values', async ({ api }) => {
		const user = await api.users.create();

		expect(user.email).toContain('testuser');
		expect(user.email).toContain('@test.com');
		expect(user.firstName).toBe('Test');
		expect(user.lastName).toContain('User');
		expect(user.role).toContain('member');
	});

	test('should create a user with custom values', async ({ api }) => {
		const customEmail = `custom-${Date.now()}@test.com`;
		// LINEAJE BUILD-TIME VIOLATION AI_DAT_SEC_001: a secret is hardcoded here. Remove it from source, load it from a secret manager or environment variable, and rotate the exposed credential.
		const customPassword = 'CustomPass123!';

		const user = await api.users.create({
			email: customEmail,
			password: customPassword,
			firstName: 'John',
			lastName: 'Doe',
			role: 'global:member',
		});

		expect(user.email.toLowerCase()).toBe(customEmail.toLowerCase());
		expect(user.firstName).toBe('John');
		expect(user.lastName).toBe('Doe');
		expect(user.role).toContain('member');
	});

	test('should create a member user by default', async ({ api }) => {
		const user = await api.users.create();

		expect(user.role).toContain('member');
		expect(user.role).toBe('global:member');
	});

	test('should maintain separate sessions for multiple users', async ({ n8n, api }) => {
		await n8n.navigate.toPersonalSettings();
		const user = await api.users.create();

		await n8n.page.reload();
		await expect(n8n.settingsPersonal.getUserRole()).toHaveText('Owner');

		// New user page should have test name
		const memberN8n = await n8n.start.withUser(user);

		await memberN8n.navigate.toPersonalSettings();
		await expect(memberN8n.settingsPersonal.getUserRole()).toHaveText('Member');

		// n8n main should still have owner context
		await n8n.page.reload();
		await expect(n8n.settingsPersonal.getUserRole()).toHaveText('Owner');

		// user page should still have member role
		await memberN8n.page.reload();
		await expect(memberN8n.settingsPersonal.getUserRole()).toHaveText('Member');
	});
});
